# ClojoSolace
ClojoSolace is a lightweight Clojure wrapper around Solace Java messaging API

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
  "Use ^Connection `conn` to send a request to `topic` with `payload`. Return a promesa promise
   that will resolve to ^InboundMsg transformed by `preprocess` (default: identity) function, or
   rejected as {:user-context ^Object, :error ^PubSubPlusClientException} if exception occurred."
  [^Connection conn & {:keys [topic payload
                              timeout_ms      preprocess]
                       :or   {timeout_ms 3000 preprocess identity}}] ...)

(defn listen-requests
  "Use ^Connection `conn` to listen to requests sent to `topic` and returns a promesa channel.
   Item in channel will resolve to:
       {:message ^InboundMessage transformed by `preprocess` (default: identity) function,
        :reply   (fn [payload] ...) for sending payload as reply to requestor}.
   `buffer` (default: 128) is used to construct returned channel (see promesa.exe.csp/chan for
   more info)."
  [^Connection conn & {:keys [topic preprocess          buffer]
                       :or         {preprocess identity buffer 128}}] ...)

(defn publish
  "Use ^Connection `conn` to publish a message with `payload` to `topic`."
  [^Connection conn & {:keys [topic payload]}] ...)

(defn subscribe
  "Use ^Connection `conn` to subscribe to `topic` and returns a promesa channel. Item in channel
   will resolve to ^InboundMessage transformed by `preprocess` (default: identity)  function.
   `buffer` (default: 128) is used to construct returned channel (see promesa.exe.csp/chan for more
   info)."
  [conn & {:keys [topic preprocess          buffer]
           :or         {preprocess identity buffer 128}}] ...)
```

## Helper Preprocess Functions
You can supply helper preprocess functions to be used for above main API function `preprocess` param. Below are 2 simple ones provided by the API.
```clojure
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
```

# Sample Usage
See src/clojosolace/example/pubsub_reqresp.clj
