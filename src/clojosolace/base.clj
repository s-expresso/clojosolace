(ns clojosolace.base
  "Clojure wrapper for Solace Messaging API. This namespace provides a low-level API for interacting with Solace
Messaging API. For outgoing messages, use `publish`, `request` or `reply` function to send a `payload` of type
string or bytes. For incoming messages, provide a callback to `subscribe` (no reply) or `listen` (with reply)
functions which also return a `receiver` object which has .terminate method to end the subscription/listening.
   
   Note that this API is not symmetric. For outgoing messages, `payload` is a string or bytes. For incoming messages,
callback recieves a map with `:message` key which is an InboundMessage object. Use `get-payload-as-str` or
`get-payload-as-byte` to get the payload as string or bytes respectively.

   For a higher-level API that is not callback based, see clojosolace.core namespace."
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
           (java.util Properties)))

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

(defn- payload->msg [^Connection conn payload] (-> conn
                                                   :msg-svc
                                                   .messageBuilder
                                                   (.build payload)))

(defn get-payload-as-str
  [^InboundMessage msg] (.getPayloadAsString msg))

(defn get-payload-as-byte
  [^InboundMessage msg] (.getPayloadAsBytes msg))

(defn disconnect
  [^Connection conn]
  (.terminate (:publisher conn) 3000) ; grace period of 3000ms
  (.terminate (:requestor conn) 3000) ; grace period of 3000ms 
  (.disconnect (:msg-svc conn)))

; ----------------------------------------------------------------------------------------------------------------------
(defn request
  "Use ^Connection `conn` to send a request to `topic` with `payload` (string or bytes). Invokes `callback` with a map
  {:message ^InboundMsg, :user-context ^Object, :exception ^PubSubPlusClientException}. Default `timeout_ms` is 3000ms
   and can be overridden. Upon tieout, `callback` is invoked with `:exception` set to `TimeOutError`."
  [^Connection conn callback & {:keys [topic payload
                                       timeout_ms]
                                :or   {timeout_ms 3000}}]
  (let [msg (payload->msg conn payload)
        cb (reify RequestReplyMessagePublisher$ReplyMessageHandler
             (^void onMessage [this ^InboundMessage message ^Object userContext ^PubSubPlusClientException exception]
               (callback {:message message :user-context userContext :exception exception})))]
    (.publish (:requestor conn) msg cb (Topic/of topic) timeout_ms)))

; ----------------------------------------------------------------------------------------------------------------------

(defn listen
  "Use ^Connection `conn` to listen to requests sent to `topic`.  Invokes `callback` with a map {:messaage ^InboundMessage,
   :replier ^RequestReplyMessageReceiver$Replier}. Returns a receiver object for (.terminate receiver) when done."
  [^Connection conn callback & {:keys [topic]}]
  (let [receiver (-> (:msg-svc conn)
                     .requestReply
                     .createRequestReplyMessageReceiverBuilder
                     (.build (TopicSubscription/of topic))
                     .start)
        cb (reify RequestReplyMessageReceiver$RequestMessageHandler
             (^void onMessage [this ^InboundMessage message ^RequestReplyMessageReceiver$Replier replier]
               #_(println "listen received" message)
               (callback {:message message :replier replier})))]
    (.receiveAsync receiver cb)
    receiver))

(defn reply
  "Reply to `incoming` message with `payload`."
  [conn incoming payload]
  (.reply (:replier incoming) (payload->msg conn payload)))

; ----------------------------------------------------------------------------------------------------------------------

(defn subscribe
  "Use ^Connection `conn` to subscribe to `topic` and receives data via `callback`. Invokes `callback` with a map
  {:message ^InboundMsg, :user-context ^Object, :exception ^PubSubPlusClientException}. Returns a receiver object for
  (.terminate receiver) when done."
  [conn callback & {:keys [topic]}]
  (let [receiver (-> (:msg-svc conn)
                     .createDirectMessageReceiverBuilder
                     (.withSubscriptions (into-array [(TopicSubscription/of topic)]))
                     (.build)
                     .start)
        cb (reify MessageReceiver$MessageHandler
             (^void onMessage [this ^InboundMessage message]
               (callback {:message message})))]
    (.receiveAsync receiver cb)
    receiver))

; ----------------------------------------------------------------------------------------------------------------------

(defn publish
  "Use ^Connection `conn` to publish a message with `payload` to `topic`."
  [^Connection conn & {:keys [topic payload]}]
  (.publish (:publisher conn) payload (Topic/of topic)))

; ----------------------------------------------------------------------------------------------------------------------

(defn unsubscribe
  "Cancel subscription. `chan` param must be a channel returned by `subscribe` or `listen` function."
  [receiver]
  (.terminate receiver 1))