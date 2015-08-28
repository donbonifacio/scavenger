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

(defn- log
  "Logs status"
  [data status]
  (print data)
  (flush))

(defn- make-response
  [deferred]
  (try
    (let [response @(d/chain deferred)]
      {:response response
       :body (slurp (:body response))})
    (catch Exception e
      #_(println (.getMessage e))
      {:exception true})))

(defn read-lines [ch file]
  (go
    (with-open [rdr (clojure.java.io/reader file)]
      (loop []
        (if-let [line (.readLine rdr)]
          (do (log \. :success)
              (>! ch line)
              (recur))
          (do (log \x :close-channel)
              (close! ch)))))))

(defn- fetch-response
  "Fetches the response"
  [url]
  (try
    (let [async-stream (http/get (str "http://" url) {:connection-timeout 5000
                                                      :request-timeout 5000})
          c (chan)]
      #_(s/connect async-stream c)
      (d/on-realized async-stream
                         (fn [x] (if x (>!! c x) (close! c)))
                             (fn [x] (close! c)))
      c)
    (catch Exception e
      (log \E :exception))))

(defn get-responses
  "Given a coll of urls, will load the http responses"
  [urls-chan responses-chan]
  (go-loop []
    (let [url (<! urls-chan)
          urls-batch [url]]
      (if (not url)
        (do
          (log \x :close-channel)
          (close! responses-chan))
        (do
          (if-let [c (fetch-response url)]
            (when-let [response (<! c)]
              (log \: :success)
              (>! responses-chan response))))))
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
  (let [responses-workers 10
        reports-workers 10
        urls-chan (chan responses-workers)
        responses-chan (chan reports-workers)
        file-name "data/sample.txt"
        reader (read-lines urls-chan file-name)
        responses (dotimes [n responses-workers] (get-responses urls-chan responses-chan))]
    (<!! (reporter responses-chan))))
