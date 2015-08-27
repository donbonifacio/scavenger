(ns scavenger.core
  (:gen-class)
  (:require
    [compojure.core :as compojure :refer [GET]]
    [ring.middleware.params :as params]
    [compojure.route :as route]
    [aleph.http :as http]
    [byte-streams :as bs]
    [manifold.stream :as s]
    [manifold.deferred :as d]
    [clojure.core.async :as async :refer :all 
     :exclude [map into reduce merge partition partition-by take]]))

(defn- make-response
  [deferred]
  (try
    (let [response @(d/chain deferred)]
      {:response response
       :body (slurp (:body response))})
    (catch Exception e
      #_(println (.getMessage e))
      {:exception true})))

(defn read-lines [ch batch-size file]
  (go
    (with-open [rdr (clojure.java.io/reader file)]
      (loop []
        (if-let [line (.readLine rdr)]
          (do (print \.)
              (>! ch line)
              (recur))
          (do (print \x)
              (close! ch)))))))

(defn get-responses
  "Given a coll of urls, will load the http responses"
  [urls-chan responses-chan]
  (go-loop []
    (let [url (<! urls-chan)
          urls-batch [url]]
      (if (not url)
        (do
          (print \x)
          (close! responses-chan))
        (do
          (println "HERE")
          (try
            (let [async-stream (http/get (str "http://" url) {:connection-timeout 5000
                                                                :request-timeout 5000})
                  c (chan)
                  _ (s/connect async-stream c)
                  responses (<! c)]
              (print \:)
              (>! responses-chan responses))
            (catch Exception e
              (println "HERE I AM")
              (print \E))))))
    (recur)))

(defn- reporter
  "Reports data on the given channel"
  [ch]
  (go-loop []
    (let [data (<! ch)]
      (if (seq data)
        (recur)
        (do
          (println "[reporter] End!")
          (shutdown-agents))))))

(defn -main
  [& args]
  (let [batch-size 10
        urls-chan (chan 1)
        responses-chan (chan 1)
        file-name "data/sample.txt"
        reader (read-lines urls-chan batch-size file-name)
        responses (get-responses urls-chan responses-chan)]
    (<!! (reporter responses-chan))
    #_(shutdown-agents)))
