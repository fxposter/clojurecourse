(ns task02.helpers)

(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defn dropv [n coll]
  (vec (drop n coll)))
