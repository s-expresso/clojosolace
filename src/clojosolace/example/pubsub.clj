(ns clojosolace.example.pubsub
  (:require [clojosolace.core :as core]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

(defn ->msg [msg]
  (let [msg (:message msg)
        topic (.getDestinationName msg)
        content (.getPayloadAsString msg)]
    {:topic topic :payload content}))

; to run these examples, change config below to connect to your solace event broker
(def svc (core/connect {:host "host[:port]" :vpn "vpn" :user "user" :password "password"}))

; subscribe to a topic and apply ->msg to payload in {:message payload, ...} before it is put into returned channel
(def sub-chan (core/subscribe svc :topic "a/b/c"))
; => {:chan ..., :receiver ...}

; publish a message
(def msg-data {:topic "a/b/c" :payload "hello"})
(core/publish svc msg-data)
(println "Published:" msg-data)

; read from subscription channel
(def p (-> sub-chan
           (csp/take 1000) ; timeout 1000ms
           (p/catch Exception #(str "Subscription exception: " %))))

(println "Received: " (->msg @p))

; clean up
(core/unsubscribe svc sub-chan)
(core/disconnect svc)


