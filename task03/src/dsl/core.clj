(ns dsl.core
  (:use clojure.walk))

(def cal (java.util.Calendar/getInstance))
(def today (java.util.Date.))
(def yesterday (do (.add cal java.util.Calendar/DATE -1) (.getTime cal)))
(def tomorrow (do (.add cal java.util.Calendar/DATE 2) (.getTime cal)))

(comment
  (defn one [] 1)

  ;; Примеры вызова
  (with-datetime
    (if (> today tomorrow) (println "Time goes wrong"))
    (if (<= yesterday today) (println "Correct"))
    (let [six (+ 1 2 3)
          d1 (today - 2 days)
          d2 (today + 1 week)
          d3 (today + six months)
          d4 (today + (one) year)]
      (if (and (< d1 d2)
               (< d2 d3)
               (< d3 d4))
        (println "DSL works correctly")))))

(def ^:private date-periods {'second java.util.Calendar/SECOND,
                             'seconds java.util.Calendar/SECOND,
                             'minute java.util.Calendar/MINUTE,
                             'minutes java.util.Calendar/MINUTE,
                             'hour java.util.Calendar/HOUR,
                             'hours java.util.Calendar/HOUR,
                             'day java.util.Calendar/DATE,
                             'days java.util.Calendar/DATE,
                             'week java.util.Calendar/WEEK_OF_YEAR,
                             'weeks java.util.Calendar/WEEK_OF_YEAR,
                             'month java.util.Calendar/MONTH,
                             'months java.util.Calendar/MONTH,
                             'year java.util.Calendar/YEAR,
                             'years java.util.Calendar/YEAR})

(def ^:private date-comparison-operators '#{> < >= <=})

(def ^:private thread-local-calendar
  (proxy [ThreadLocal] []
    (initialValue []
      (java.util.Calendar/getInstance))))

(defn- calendar [] (.get thread-local-calendar))

(defn modify-date [date field amount]
  (->
    (doto (calendar)
      (.setTime date)
      (.add field amount))
    .getTime))

(defn- date? [date]
  (instance? java.util.Date date))

(defn compare-dates-or-numbers [op date-or-number1 date-or-number2]
  (if (and (date? date-or-number1)
           (date? date-or-number2))
    (op (.compareTo date-or-number1 date-or-number2) 0)
    (op date-or-number1 date-or-number2)))

(defn- matches-date-modification-dsl? [code]
  (and (list? code)
       (= (count code) 4)
       (date-periods (last code))))

(defn- transform-date-modification-dsl [[date op count period]]
  `(modify-date ~date ~(date-periods period) (~op ~count)))

(defn- apply-modification-transformations [code]
  (if (matches-date-modification-dsl? code)
    (transform-date-modification-dsl code)
    code))

(defn- matches-date-comparison-dsl? [code]
  (and (list? code)
       (= (count code) 3)
       (date-comparison-operators (first code))))

(defn- transform-date-comparison-dsl [[op date1 date2]]
  `(compare-dates-or-numbers ~op ~date1 ~date2))

(defn- apply-comparison-transformations [code]
  (println code)
  (if (matches-date-comparison-dsl? code)
    (transform-date-comparison-dsl code)
    code))

(defn- transform-datetime-dsl [code]
  (postwalk
    (comp
      apply-modification-transformations
      apply-comparison-transformations)
    code))

;; Режим Бога -- никаких подсказок.
;; Вы его сами выбрали ;-)
(defmacro with-datetime [& code]
  `(do ~@(transform-datetime-dsl code)))
