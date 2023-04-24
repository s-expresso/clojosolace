(ns clojosolace.example.pubsub-reqresp
  (:require [clojosolace.messaging :as sol]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

; to run these examples, change config to connect to your solace event broker
(def config {:host "host:port" :vpn "vpn" :user "user" :password "password"})

(defn publish-subscribe []
  (let [conn (sol/connect config)
        subscription (sol/subscribe conn :topic "a/b/c" :preprocess sol/payload->str)
        subscription-promise (-> (csp/take subscription 3000)
                                 (p/catch Exception #(str "subscription exception: " %)))]
    (sol/publish conn :topic "a/b/c" :payload "qwerty")
    (println "received msg via publish-subscribe: " @subscription-promise)
    (sol/disconnect conn)))


(defn request-respond []
  (let [conn (sol/connect config)
        listen-req-chan (sol/listen-requests conn :topic "a/b/c" :preprocess sol/payload->str)
        listen-req-promise (-> (csp/take listen-req-chan 3000)
                               (p/chain #((:reply %) (str "echo " (:message %))))
                               (p/catch Exception #(str "respond exception: " %)))
        respond (-> (sol/request conn :topic "a/b/c" :payload "qwerty" :preprocess sol/payload->str)
                    (p/catch Exception #(str "exception: " %)))]
    (println @listen-req-promise)
    (println "received msg via request-respond: " @respond)
    (sol/disconnect conn)))

(publish-subscribe)

(request-respond)
