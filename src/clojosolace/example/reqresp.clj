(ns clojosolace.example.reqresp
  (:require [clojosolace.base :as base]
            [clojosolace.core :as sol]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

; to run these examples, change config below to connect to your solace event broker
(def svc (sol/connect {:host "host[:port]" :vpn "vpn" :user "user" :password "password"}))

; listen to a/b/c topic for request
(def chan (sol/listen svc :topic "a/b/c"))

; handle a request {:message ..., :replier ...} where :message is the content and :replier is an opaque object.
(def handle-req (-> (csp/take chan 3000)
                    (p/chain ->msg
                             #(sol/reply svc % (str "replied " (:message %)))) ; reply
                    (p/catch Exception #(str "respond exception: " %))))

; send a request, and p/chain on returned promise to preprocess return
(def req {:topic "a/b/c" :payload "qwerty"})
(def response (-> (sol/request svc req)
                  (p/catch Exception #(str "exception: " %))))
(println "Requested:" req)

; wait for request and response to complete
@handle-req
(println "\nRceived reply:" @response)

(defn ->msg [msg] (->> (msg :message)
                       (base/get-payload-as-str)
                       (assoc msg :message)))

(println "\nExtracted reply:" (->msg @response))

(sol/unsubscribe svc chan)
(sol/disconnect svc)
