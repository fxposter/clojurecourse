(ns task02.match-regex
  (:require [clojure.core.match :refer (emit-pattern to-source)]))

(defmethod emit-pattern java.util.regex.Pattern [pat]
  pat)

(defmethod to-source java.util.regex.Pattern [pat ocr]
  `(re-matches ~pat ~ocr))
