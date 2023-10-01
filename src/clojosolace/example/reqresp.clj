(ns clojosolace.example.reqresp
  (:require [clojosolace.messaging :as sol]
            [promesa.core :as p]
            [promesa.exec.csp :as csp]))

; to run these examples, change config below to connect to your solace event broker
(def conn (sol/connect {:host "host[:port]" :vpn "vpn" :user "user" :password "password"}))

; listen to a/b/c topic for request
(def sub (sol/listen conn :topic "a/b/c"))
; => {:chan ..., :receiver ...}

; handle a request {:message ..., :reply ...} where :message is the content and :reply contains a function
; for replying to requestor.
(def handle-req (-> (csp/take (sub :chan) 3000)
                    (p/chain #((:reply %) (str "handled " (sol/payload->str (:message %))))) ; reply
                    (p/catch Exception #(str "respond exception: " %))))

; send a request, and p/chain on returned promise to preprocess return
(def payload "qwerty")
(def response (-> (sol/request conn :topic "a/b/c" :payload payload)
                  (p/chain #(sol/payload->str %))
                  (p/catch Exception #(str "exception: " %))))
(println "Requested:" payload)

; wait for request and response to complete
@handle-req
(println "Rceived response:" @response)

(.terminate (sub :receiver) 1000)
(sol/disconnect conn)
