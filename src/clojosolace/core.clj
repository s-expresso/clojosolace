(ns clojosolace.core
  "Wraps clojosolace.base namespace with a higher-level API that returns promise or channel.
   
For outgoing messages, use `publish`, `reply` or `request` function to send a `payload` of type string or bytes. `request`
returns a promise that will be resolved with {:message ..., :user-context ..., :exception ...}. `publish` and `reply`
do not return anything.

For incoming messages, use `subscribe` (no reply) or `listen` (with reply). `subscribe` returns a channel that will receive
{:message ...} and `listen` returns a channel that will receive {:message ..., :replier ...}. For latter, client shall user `reply`
function to reply to request."

  (:require [clojosolace.base :as base]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

(defrecord Service [connection subscriptions])

(defn- promesa-promise-factory
  "Return [promise callback] where `callback` has signature (fn [reply-msg] ...) and will put `reply-msg` into `promise`.
`reply-msg` is a map with {:message ..., :replier ...} and caller can optionally provide named param :preprocess with a
function with 1-arity to transform message before it is put into promise."
  [options]
  (let [{:keys [preprocess]
         :or   {preprocess identity}} options
        promise (p/deferred)
        callback (fn [reply-msg] (p/resolve! promise (preprocess reply-msg)))]
    [promise callback]))

(defn- promesa-chan-factory
  "Return [channel callback] where callback will put incoming message into channel, where message for
request   - {:message ..., :user-context ..., :exception ...}
listen    - {:message ..., :replier ...}
subscribe - {:message ...}

Caller can optionally provide named params :buffer to customize channel and :preprocess to provide a 
function with 1-arity to transform message before it is put into channel."
  [options]
  (let [{:keys [buffer     preprocess]
         :or   {buffer 128 preprocess identity}} options
        chan (csp/chan buffer)
        callback (fn [msg]
                   (let [msg (preprocess msg)]
                     #_(println " callback " msg)
                     (csp/put chan msg)))]
    [chan callback]))

; ----------------------------------------------------------------------------------------------------------------------

(defn connect
  "Connect to solace. Opts map must contain :host, :vpn, :user and :password, and can optionally contain :reconnections
(default 20), reconnections-per-host (default 5), and :receiver-direct-sub-reapply (default true)."
  [opts] (->Service (base/connect opts) (atom {})))

; ----------------------------------------------------------------------------------------------------------------------

(defn publish "Publish `message-info` {:topic topic :payload payload}, where `payload` is string or bytes."
  [^Service svc message-info] (base/publish (:connection svc) message-info))

; ----------------------------------------------------------------------------------------------------------------------

(defn request
  "Use `svc` to send a request to `topic` with `payload` with `timeout_ms`,  where `payload` is string or bytes.

Named variable `chan-factory` expects a function (fn [options] ...) that returns [promise (fn [reply-msg] ...)], where `fn`
is expected to put `reply-msg` into `promise`. Mapped to `promesa-promise-factory` by default.
   
`reply-msg` is a map with {:message ..., :user-context ..., :exception ...}.

Named variables are collected into `options` which is then passed into `promise-factory` function, typically used for
preprocessing `reply-msg` before it is put into promise."
  [^Service svc & {:keys [topic payload
                          timeout_ms      promise-factory]
                   :or   {timeout_ms 3000 promise-factory promesa-promise-factory}
                   :as   options}]
  (let [[promise callback] (promise-factory options)]
    (base/request (:connection svc) callback :topic topic :payload payload :timeout_ms timeout_ms)
    promise))

; ----------------------------------------------------------------------------------------------------------------------

(defn listen
  "Use `svc` to listen to requests sent to `topic` and returns a channel. Client shall use channel to receive data and
call (unsubscribe svc channel) when done.

Named variable `chan-factory` expects a function (fn [options] ...) that returns [channel (fn [request-msg] ...)],
where `fn` is expected to put `request-msg` into `channel`. Mapped to `promesa-chan-factory` by default.
   
`request-msg` is a map with {:message ..., :replier ...} where :replier is an opaque object which you shouldn't interact
with directly. To respond, call `(reply svc request-msg payload)`.

Named variables are collected into `options `which is then passed into `chan-factory` function, typically used for
preprocessing `msg` before it is put into `channel` and also to customize the `channel`."
  [^Service svc & {:keys [topic chan-factory]
                   :or         {chan-factory promesa-chan-factory}
                   :as options}]
  (let [[chan callback] (chan-factory options)
        receiver (base/listen (:connection svc) callback :topic topic)]
    (swap! (:subscriptions svc) #(conj % {chan receiver}))
    chan))

; ----------------------------------------------------------------------------------------------------------------------

(defn reply
  "Use `svc` to reply to `incoming` request {:message ..., replier ...} with `payload` (string or bytes)."
  [^Service svc incoming payload]
  (base/reply (:connection svc) incoming payload))

; ---------------------------------------------------------------------------------------------------------------------

(defn subscribe
  "Use `svc` to subscribe to named variable `topic` and returns a channel. Client shall use `channel` to receive data 
and call (unsubscribe svc channel) when done.

Named variable `chan-factory `expects a function (fn [options] ...) that returns [channel (fn [msg] ...)], where `fn`
is expected to put `msg` into `channel`. Mapped to `promesa-chan-factory `by default.
   
`msg` is a map with {:message ...}.

Named variables are collected into `options `which is then passed into `chan-factory` function, typically used for
preprocessing `msg` before it is put into `channel` and also to customize the `channel`."
  [^Service svc & {:keys [topic chan-factory]
                   :or         {chan-factory promesa-chan-factory}
                   :as options}]
  (let [[chan callback] (chan-factory options)
        receiver (base/subscribe (:connection svc) callback :topic topic)]
    (swap! (:subscriptions svc) conj {chan receiver})
    chan))

; ---------------------------------------------------------------------------------------------------------------------

(defn unsubscribe "Unsbuscribe channel from service."
  [^Service svc chan]
  (let [subscriptions (:subscriptions svc)
        receiver (@subscriptions chan)]
    (swap! subscriptions dissoc chan)
    (base/unsubscribe receiver)))

; ---------------------------------------------------------------------------------------------------------------------

(defn disconnect "Disconnect service. Unsubscribe all subscriptions and disconnect connection."
  [^Service svc]
  (let [[subs _] (reset-vals! (:subscriptions svc) {})]
    (run! (fn [x] (base/unsubscribe (second x))) subs)
    (base/disconnect (:connection svc))))
