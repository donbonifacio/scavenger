(ns scavenger.core
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
    (catch Exception e)))

(defn read-lines [ch batch-size file]
  (go
    (with-open [rdr (clojure.java.io/reader file)]
      (loop [lines []]
        (if (<= batch-size (count lines))
          (do (>! ch lines)
              (recur []))
          (if-let [line (.readLine rdr)]
            (recur (conj lines line))
            (do (>! ch lines)
                (close! ch)))))
      (println "No more urls!"))))

(defn get-responses
  "Given a coll of urls, will load the http responses"
  [urls-chan responses-chan])

(defn run
  []
  (let [batch-size 12
        urls-chan (chan 5)
        responses-chan (chan)
        reader (read-lines urls-chan batch-size "data/sample.txt")
        responses (get-responses urls-chan responses-chan)]
    (go-loop []
      (when-let [urls-batch (<! urls-chan)]
        (println "Fetched" (count urls-batch) "urls...")
        (if (<= batch-size (count urls-batch))
          (recur)
          (shutdown-agents))))))


;(mapv http/get)
;(mapv make-response)
;(println)))))
