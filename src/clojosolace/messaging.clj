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

(defn request
  "Use ^Connection `conn` to send a request to `topic` with `payload`. Return a promesa promise
   that will resolve to ^InboundMsg transformed by `preprocess` (default: identity) function, or
   rejected as {:user-context ^Object, :error ^PubSubPlusClientException} if exception occurred."
  [^Connection conn & {:keys [topic payload
                              timeout_ms      preprocess]
                       :or   {timeout_ms 3000 preprocess identity}}]
  (let [msg-builder (.messageBuilder (:msg-svc conn))
        msg (-> msg-builder (.build payload))
        promise (p/deferred)
        callback (reify RequestReplyMessagePublisher$ReplyMessageHandler
                   (^void onMessage [this ^InboundMessage message ^Object userContext ^PubSubPlusClientException exception]
                     (if (nil? exception)
                       (p/resolve! promise (preprocess message))
                       (p/reject! promise (ex-info "request-err" {:user-context userContext :error exception})))))]
    (.publish (:requestor conn) msg callback (Topic/of topic) timeout_ms)
    promise))

(defn listen-requests
  "Use ^Connection `conn` to listen to requests sent to `topic` and returns a promesa channel.
   Item in channel will resolve to:
       {:message ^InboundMessage transformed by `preprocess` (default: identity) function,
        :reply   (fn [payload] ...) for sending payload as reply to requestor}.
   `buffer` (default: 128) is used to construct returned channel (see promesa.exe.csp/chan for
   more info)."
  [^Connection conn & {:keys [topic preprocess          buffer]
                       :or         {preprocess identity buffer 128}}]
  (let [receiver (-> (:msg-svc conn)
                     .requestReply
                     .createRequestReplyMessageReceiverBuilder
                     (.build (TopicSubscription/of topic))
                     .start)
        chan (csp/chan buffer)
        ->msg #(-> (:msg-svc conn)
                   .messageBuilder
                   (.build %))
        callback (reify RequestReplyMessageReceiver$RequestMessageHandler
                   (^void onMessage [this ^InboundMessage message ^RequestReplyMessageReceiver$Replier replier]
                     (csp/put chan {:message (preprocess message)
                                    :reply #(.reply replier (->msg %))})))]
    (.receiveAsync receiver callback)
    chan))

(defn publish
  "Use ^Connection `conn` to publish a message with `payload` to `topic`."
  [^Connection conn & {:keys [topic payload]}]
  (.publish (:publisher conn) payload (Topic/of topic)))

(defn subscribe
  "Use ^Connection `conn` to subscribe to `topic` and returns a promesa channel. Item in channel
   will resolve to ^InboundMessage transformed by `preprocess` (default: identity)  function.
   `buffer` (default: 128) is used to construct returned channel (see promesa.exe.csp/chan for more
   info)."
  [conn & {:keys [topic preprocess          buffer]
           :or         {preprocess identity buffer 128}}]
  (let [receiver (-> (:msg-svc conn)
                     .createDirectMessageReceiverBuilder
                     (.withSubscriptions (into-array [(TopicSubscription/of topic)]))
                     (.build)
                     .start)
        chan (csp/chan buffer)
        callback (reify MessageReceiver$MessageHandler
                   (^void onMessage [this ^InboundMessage message]
                     (csp/put chan (preprocess message))))]
    (.receiveAsync receiver callback)
    chan))
