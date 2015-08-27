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
      (loop [lines []]
        (if-let [line (.readLine rdr)]
          (do (println (str "[read-lines] >!"))
              (>! ch line)
              (recur []))
          (do (println "[read-lines] close!")
              (close! ch)))))
      (println "[read-lines] No more urls!")))

(defn get-responses
  "Given a coll of urls, will load the http responses"
  [urls-chan responses-chan]
  (go-loop []
    (let [url (<! urls-chan)
          urls-batch [url]]
      (if (not url)
        (do
          (println "[get-responses] close!")
          (close! responses-chan))
        (do
          (println (str "[get-responses] <! "))
          (let [handles (mapv #(http/get (str "http://" %) {:connection-timeout 500
                                                            :request-timeout 500}) urls-batch)
                responses (time (mapv make-response handles))]
            (println (str "[get-responses] >! "))
            (>! responses-chan responses))
            (recur))))))

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
