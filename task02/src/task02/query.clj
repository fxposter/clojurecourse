(ns task02.query
  (:use [clojure.core.match :only (match emit-pattern to-source)]
        [task02 helpers db]))

(defrecord RegexPattern [regex])

(defmethod emit-pattern java.util.regex.Pattern [pat]
  (task02.query.RegexPattern. pat))

(defmethod to-source task02.query.RegexPattern [pat ocr]
  `(re-matches ~(:regex pat) ~ocr))

(defn make-where-function [field op value]
  (fn [entry]
    ((resolve (symbol op)) ((keyword field) entry) (read-string value))))

(defn- parse-where-clause [[v options :as original]]
  (match v
    [#"(?i)where" field op value & args] [args (assoc options :where (make-where-function field op value))]
    :else original))

(defn- parse-order-by-clause [[v options :as original]]
  (match v
    [#"(?i)order" #"(?i)by" field & args] [args (assoc options :order-by (keyword field))]
    :else original))

(defn- parse-limit-clause [[v options :as original]]
  (match v
    [#"(?i)limit" limit & args] [args (assoc options :limit (parse-int limit))]
    :else original))

(defn- parse-join-clause [[v options :as original]]
  (match v
    [#"(?i)join" table #"(?i)on" f1 "=" f2 & args]
      (let [join-args [(keyword f1) table (keyword f2)]
            updated-options (update-in options [:joins] (fnil conj []) join-args)]
        (recur [args updated-options]))
    :else original))

(defn- check-parse-result [[v options]]
  (if (empty? v)
    options
    nil))

(defn- parse-key-value-pairs [[v options :as original]]
  (match v
    [key "=" value & args]
      (let [updated-options (update-in options [:values] (fnil assoc {}) (keyword key) (read-string value))]
        (recur [args updated-options]))
    :else original))

(defn- parse-select-clauses [v]
  (-> [v {}]
      parse-where-clause
      parse-order-by-clause
      parse-limit-clause
      parse-join-clause
      check-parse-result))

(defn- parse-delete-clauses [v]
  (-> [v {}]
      parse-where-clause
      check-parse-result))

(defn- parse-insert-clauses [v]
  (-> [v nil]
      parse-key-value-pairs
      check-parse-result))

(defn- parse-update-clauses [v]
  (-> [v nil]
      parse-key-value-pairs
      parse-where-clause
      check-parse-result))

(defn parse-query [v]
  (match v
    [#"(?i)select" table & args]
      (when-let [options (parse-select-clauses args)]
        (list* 'select table (mapcat identity options)))

    [#"(?i)delete" table & args]
      (when-let [options (parse-delete-clauses args)]
        (list* 'delete table (mapcat identity options)))

    [#"(?i)insert" #"(?i)into" table #"(?i)values" & args]
      (when-let [options (parse-insert-clauses args)]
        (list* 'insert table (list (:values options))))

    [#"(?i)update" table #"(?i)set" & args]
      (when-let [options (parse-update-clauses args)]
        (list* 'update table
          (list*
            (:values options)
            (mapcat identity (remove #(= :values (% 0)) options)))))

    [#"(?i)drop" #"(?i)table" table]
      (list 'drop-table table)

    [#"(?i)create" #"(?i)table" table]
      (list 'create-table table)

    :else nil))

;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;; INSERT INTO table_name VALUES column1 = value1 column2 = value2
;; UPDATE table_name SET column1 = value1 column2 = value2 [WHERE column comp-op value]
;; DELETE table_name [WHERE column comp-op value]
;; CREATE TABLE table_name
;; DROP TABLE table_name
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-query-string "select student")
;; (select "student")
;; > (parse-query-string "select student where id = 10")
;; (select "student" :where #<function>)
;; > (parse-query-string "select student where id = 10 limit 2")
;; (select "student" :where #<function> :limit 2)
;; > (parse-query-string "select student where id = 10 order by id limit 2")
;; (select "student" :where #<function> :order-by :id :limit 2)
;; > (parse-query-string "select student where id = 10 order by id limit 2 join subject on id = sid")
;; (select "student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-query-string "werfwefw")
;; nil
;; > (parse-query-string "update student set id = 10 name = \"20\" where a = b")
;; (update "student" {:name "20", :id 10} :where #<query$make_where_function$fn__1900 task02.query$make_where_function$fn__1900@48c5a439>)
;; > (parse-query-string "delete student where a = b")
;; (delete "student" :where #<query$make_where_function$fn__1900 task02.query$make_where_function$fn__1900@3c4ac33d>)
;; > (parse-query-string "insert into student values a = \"b\" c = \"d\"")
;; (insert "student" {:c "d", :a "b"})
;; > (parse-query-string "create table group")
;; (create-table "group")
;; > (parse-query-string "drop table group")
;; (drop-table "group")
;; nil
(defn parse-query-string [^String sel-string]
  (parse-query (vec (.split sel-string " +"))))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [[action table-name & args] (parse-query-string sel-string)]
    (let [table (if (.contains '(select insert update delete) action)
                  (get-table table-name)
                  table-name)]
      (apply (ns-resolve 'task02.db action) table args))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
