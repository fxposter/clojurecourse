(ns task02-client.core
  (:gen-class)
  (:import [java.net Socket])
  (:require [clojure.java.io :as io]))

(defn create-threads [thread-count callback]
  (repeatedly thread-count #(Thread. callback)))

(defn benchmark [thread-count callback]
  (let [threads (create-threads thread-count callback)]
    (time
      (do
        (doseq [thread threads]
          (.start thread))
        (doseq [thread threads]
          (.join thread))))))

(defn connect [host port]
  (Socket. host port))

(defn query [host port message]
  (let [connection (connect host port)]
    (try
      (binding [*in* (io/reader (.getInputStream connection))
                *out* (io/writer (.getOutputStream connection))
                *flush-on-newline* true]
        (println message)
        (read-line))
      (finally
        (.close connection)))))

(defn run-queries [count host port queries]
  (loop [queries-left count
         queries-cycle (cycle queries)]
    (when-not (zero? queries-left)
      (query host port (first queries-cycle))
      (recur (dec queries-left) (next queries-cycle)))))

(defn -main []
  (let [queries ["select student"
                 "select student order by year"
                 "select student where id > 1"
                 "select student join student-subject on id = student_id join subject on subject_id = id"]
        queries-count 2520]

    (doseq [query-string queries]
      (println "Query:" query-string)
      (println (query "localhost" 9997 query-string)))

    (loop [i 1]
      (when (<= i 10)
        (println "Running" i "threads with" (/ queries-count i) "queries each")
        (benchmark i #(run-queries (/ queries-count i) "localhost" 9997 queries))
        (recur (inc i))))))
