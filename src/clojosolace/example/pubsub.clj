(ns clojosolace.example.pubsub
  (:require [clojosolace.messaging :as sol]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

; to run these examples, change config below to connect to your solace event broker
(def conn (core/connect {:host "host[:port]" :vpn "vpn" :user "user" :password "password"}))

; subscribe to a topic and apply `xf` to message payload before it is put into returned channel
(def sub (sol/subscribe conn
                        :topic "a/b/c"
                        :xf (map #(->> % (sol/payload->str)))))
; => {:chan ..., :receiver ...}

; publish a message
(def payload "qwerty")
(sol/publish conn :topic "a/b/c" :payload payload)
(println "Published:" payload)

; read from subscription channel
(def p (-> (sub :chan)
           (csp/take 1000) ; timeout 1000ms
           (p/catch Exception #(str "Subscription exception: " %))))

(println "Subscription received: " @p)

; clean up
(.terminate (sub :receiver) 1000)
(sol/disconnect conn)


