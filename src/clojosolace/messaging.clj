(ns clojosolace.messaging
  (:import (com.solace.messaging MessagingService
                                 PubSubPlusClientException)
           (com.solace.messaging.config SolaceProperties$TransportLayerProperties
                                        SolaceProperties$ServiceProperties
                                        SolaceProperties$AuthenticationProperties)
           (com.solace.messaging.config.profile ConfigurationProfile)
           (com.solace.messaging.publisher RequestReplyMessagePublisher$ReplyMessageHandler)
           (com.solace.messaging.receiver InboundMessage
                                          MessageReceiver$MessageHandler
                                          RequestReplyMessageReceiver$Replier
                                          RequestReplyMessageReceiver$RequestMessageHandler)
           (com.solace.messaging.resources Topic
                                           TopicSubscription)
           (java.util Properties))
  (:require [promesa.core :as p]
            [promesa.exec.csp :as csp]))

(defrecord Connection [msg-svc requestor publisher])

(defn connect
  "Returns ^Connection."
  [& {:keys [host vpn user password
             reconnections    reconnections-per-host   receiver-direct-sub-reapply]
      :or   {reconnections 20 reconnections-per-host 5 receiver-direct-sub-reapply true}}]
  (let [properties (doto (Properties.)
                     (.setProperty SolaceProperties$TransportLayerProperties/HOST host)
                     (.setProperty SolaceProperties$ServiceProperties/VPN_NAME vpn)
                     (.setProperty SolaceProperties$AuthenticationProperties/SCHEME_BASIC_USER_NAME user)
                     (.setProperty SolaceProperties$AuthenticationProperties/SCHEME_BASIC_PASSWORD password)
                     (.setProperty SolaceProperties$TransportLayerProperties/RECONNECTION_ATTEMPTS (str reconnections))
                     (.setProperty SolaceProperties$TransportLayerProperties/CONNECTION_RETRIES_PER_HOST (str reconnections-per-host))
                     (.setProperty SolaceProperties$ServiceProperties/RECEIVER_DIRECT_SUBSCRIPTION_REAPPLY (str receiver-direct-sub-reapply)))
        msg-svc (-> (MessagingService/builder ConfigurationProfile/V1)
                    (.fromProperties properties)
                    (.build)
                    (.connect))
        requestor (-> msg-svc
                      .requestReply
                      .createRequestReplyMessagePublisherBuilder
                      .build
                      .start)
        publisher (-> msg-svc
                      .createDirectMessagePublisherBuilder
                      (.onBackPressureWait 1)
                      .build
                      .start)]
    (->Connection msg-svc requestor publisher)))

(defn payload->str
  "Extract and return ^InboundMessage's payload as string; to be used as preprocessor param for
   (request ...), (listen-request ...) and (subscribe ...).
   Example: (listen-request conn :topic \"a/b/c\" :preprocess payload->str)"
  [^InboundMessage msg] (.getPayloadAsString msg))

(defn payload->bytes
  "Extract and return ^InboundMessage's payload as bytes; to be used as preprocessor param for
   request, listen-request and subscribe.
   Example: (listen-request conn :topic \"a/b/c\" :preprocess payload->bytes)"
  [^InboundMessage msg] (.getPayloadAsBytes msg))

(defn disconnect
  [^Connection conn]
  (.terminate (:publisher conn) 3000) ; grace period of 3000ms
  (.terminate (:requestor conn) 3000) ; grace period of 3000ms
  (.disconnect (:msg-svc conn)))

; ----------------------------------------------------------------------------------------------------------------------
(defn request-callback
  "Use ^Connection `conn` to send a request to `topic` with `payload`. Invokes `callback` with 3 arity (^InboundMsg message,
   ^Object userContext, ^PubSubPlusClientException exception)."
  [^Connection conn callback & {:keys [topic payload
                                       timeout_ms]
                                :or   {timeout_ms 3000}}]
  (let [msg-builder (.messageBuilder (:msg-svc conn))
        msg (-> msg-builder (.build payload))
        cb (reify RequestReplyMessagePublisher$ReplyMessageHandler
             (^void onMessage [this ^InboundMessage message ^Object userContext ^PubSubPlusClientException exception]
               (callback message userContext exception)))]
    (.publish (:requestor conn) msg cb (Topic/of topic) timeout_ms)))

(defn- promesa-promise-factory
  "Return [promise callback] where callback will put ^InboundMessage into promise when response data arrives."
  [_]
  (let [promise (p/deferred)
        callback (fn [^InboundMessage message ^Object userContext ^PubSubPlusClientException exception]
                   (if (nil? exception)
                     (p/resolve! promise message)
                     (p/reject! promise (ex-info "request-err" {:user-context userContext :error exception}))))]
    [promise callback]))

(defn request
  "Use ^Connection `conn` to send a request to `topic` with `payload` with `timeout_ms`.
   
   Named variable `promise-factory` expects a function (fn [options] ...) that returns a promise and fn pair, where fn has 3
   arity (^InboundMsg message, ^Object userContext, ^PubSubPlusClientException exception) and must update the promise when
   invoked. The same promise is returned immediately by this function. Mapped to `promesa-promise-factory` by default.

   Named variables are collected into `options`, which is then passed into `promise-factory` function."
  [^Connection conn & {:keys [topic payload
                              timeout_ms      promise-factory]
                       :or   {timeout_ms 3000 promise-factory promesa-promise-factory}
                       :as   options}]
  (let [[promise callback] (promise-factory options)]
    (request-callback conn callback :topic topic :payload payload :timeout_ms timeout_ms)
    promise))

; ----------------------------------------------------------------------------------------------------------------------

(defn listen-callback
  "Use ^Connection `conn` to listen to requests sent to `topic`.  Invokes `callback` with 2 arity (^InboundMessage
   message ^RequestReplyMessageReceiver$Replier replier). Returns a receiver object for (.terminate receiver) when done."
  [^Connection conn callback & {:keys [topic]}]
  (let [receiver (-> (:msg-svc conn)
                     .requestReply
                     .createRequestReplyMessageReceiverBuilder
                     (.build (TopicSubscription/of topic))
                     .start)
        cb (reify RequestReplyMessageReceiver$RequestMessageHandler
             (^void onMessage [this ^InboundMessage message ^RequestReplyMessageReceiver$Replier replier]
               (callback message replier)))]
    (.receiveAsync receiver cb)
    receiver))

(defn- promesa-listen-chan-factory
  "Return [promise callback] where callback will put {:message (xf ^InboundMessage), :reply (fn ...)} into promise
   when data arrives."
  [conn options]
  (let [{:keys [buffer     xf]
         :or   {buffer 128}} options
        ->msg #(-> (:msg-svc conn)
                   .messageBuilder
                   (.build %))
        chan (csp/chan buffer xf)
        callback (fn [^InboundMessage message ^RequestReplyMessageReceiver$Replier replier]
                   (csp/put chan {:message message
                                  :reply #(.reply replier (->msg %))}))]
    [chan callback]))

(defn listen
  "Use ^Connection `conn` to listen to requests sent to `topic` and returns {:chan channel, :receiver receiver}.
   Client shall use `channel` to receive data and call (.close receiver) when done.
     
  Named variable `reqresp-factory` expects a function (fn [options] ...) that returns [channel
   (fn [^InboundMessage message ^RequestReplyMessageReceiver$Replier replier] ...)]. Mapped to `promesa-listen-chan-factory`
   by default.
  
  Named variables are collected into `options`, which is then passed into `reqresp-factory` function."
  [^Connection conn & {:keys [topic reqresp-factory]
                       :or         {reqresp-factory promesa-listen-chan-factory}
                       :as options}]
  (let [[chan callback] (reqresp-factory conn options)
        receiver (listen-callback conn callback :topic topic)]
    {:chan chan :receiver receiver}))

; ----------------------------------------------------------------------------------------------------------------------

(defn subscribe-callback
  "Use ^Connection `conn` to subscribe to `topic` and receives data via `callback`, where `callback`
   signature is (fn [^InboundMessage msg] ...)"
  [conn callback & {:keys [topic]}]
  (let [receiver (-> (:msg-svc conn)
                     .createDirectMessageReceiverBuilder
                     (.withSubscriptions (into-array [(TopicSubscription/of topic)]))
                     (.build)
                     .start)
        cb (reify MessageReceiver$MessageHandler
             (^void onMessage [this ^InboundMessage message]
               (callback message)))]
    (.receiveAsync receiver cb)
    receiver))

(defn- promesa-subscribe-chan-factory
  "Return [promise callback] where callback will put (xf ^InboundMessage) into promise when data arrives."
  [options]
  (let [{:keys [buffer     xf]
         :or   {buffer 128}} options
        chan (csp/chan buffer xf)
        callback (fn [message] (csp/put chan message))]
    [chan callback]))

(defn subscribe
  "Use ^Connection `conn` to subscribe to named variable `topic` and returns {:chan channel, :receiver receiver}.
   Client shall use `channel` to receive data and call (.terminate receiver) when done.
   
   Named variable `chan-factory` expects a function (fn [options] ...) that returns [channel
   (fn [^InboundMessage m] ...)]. Mapped to `promesa-subscribe-chan-factory` by default.

   Named variables are collected into `options`, which is then passed into `chan-factory` function."
  [conn & {:keys [topic chan-factory]
           :or         {chan-factory promesa-subscribe-chan-factory}
           :as options}]
  (let [[chan callback] (chan-factory options)
        receiver (subscribe-callback conn callback :topic topic)]
    {:chan chan :receiver receiver}))

; ----------------------------------------------------------------------------------------------------------------------

(defn publish
  "Use ^Connection `conn` to publish a message with `payload` to `topic`."
  [^Connection conn & {:keys [topic payload]}]
  (.publish (:publisher conn) payload (Topic/of topic)))"