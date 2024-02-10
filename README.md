# ClojoSolace
ClojoSolace is a lightweight Clojure wrapper around Solace Java messaging API

## Usage
Add the following to deps.edn (or its equivalent for lein).
```edn
{:deps {com.github.s-expresso/clojosolace {:mvn/version "0.0.2"}}}
```

## Main API Functions
```clojure
(defn connect
  "Returns ^Connection."
  [& {:keys [host vpn user password
             reconnections    reconnections-per-host   receiver-direct-sub-reapply]
      :or   {reconnections 20 reconnections-per-host 5 receiver-direct-sub-reapply true}}] ...)

(defn disconnect
  [^Connection conn] ...)

(defn request
  "Use ^Connection `conn` to send a request to `topic` with `payload` with `timeout_ms`.
   
   Named variable `promise-factory` expects a function (fn [options] ...) that returns a promise and fn pair, where fn has 3
   arity (^InboundMsg message, ^Object userContext, ^PubSubPlusClientException exception) and must update the promise when
   invoked. The same promise is returned immediately by this function. Mapped to `promesa-promise-factory` by default.

   Named variables are collected into `options`, which is then passed into `promise-factory` function."
  [^Connection conn & {:keys [topic payload
                              timeout_ms      promise-factory]
                       :or   {timeout_ms 3000 promise-factory promesa-promise-factory}
                       :as   options}] ...)

(defn listen
  "Use ^Connection `conn` to listen to requests sent to `topic` and returns {:chan channel, :receiver receiver}.
   Client shall use `channel` to receive data and call (.terminate receiver 1000) when done.
     
  Named variable `reqresp-factory` expects a function (fn [options] ...) that returns [channel
   (fn [^InboundMessage message ^RequestReplyMessageReceiver$Replier replier] ...)]. Mapped to `promesa-listen-chan-factory`
   by default.
  
  Named variables are collected into `options`, which is then passed into `reqresp-factory` function."
  [^Connection conn & {:keys [topic reqresp-factory]
                       :or         {reqresp-factory promesa-listen-chan-factory}
                       :as options}] ...)

(defn publish
  "Use ^Connection `conn` to publish a message with `payload` to `topic`."
  [^Connection conn & {:keys [topic payload]}] ...)

(defn subscribe
  "Use ^Connection `conn` to subscribe to named variable `topic` and returns {:chan channel, :receiver receiver}.
   Client shall use `channel` to receive data and call (.terminate receiver 1000) when done.
   
   Named variable `chan-factory` expects a function (fn [options] ...) that returns [channel
   (fn [^InboundMessage m] ...)]. Mapped to `promesa-subscribe-chan-factory` by default.

   Named variables are collected into `options`, which is then passed into `chan-factory` function."
  [conn & {:keys [topic chan-factory]
           :or         {chan-factory promesa-subscribe-chan-factory}
           :as options}] ...)
```

## Helper Preprocess Functions
```clojure
(defn payload->str
  [^InboundMessage msg] (.getPayloadAsString msg))

(defn payload->bytes
  [^InboundMessage msg] (.getPayloadAsBytes msg))
```

# Sample Usage
* src/clojosolace/example/pubsub.clj
* src/clojosolace/example/reqresp.clj
