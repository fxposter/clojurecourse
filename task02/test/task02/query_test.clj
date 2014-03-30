(ns task02.query-test
  (:require [clojure.test :refer :all]
            [task02.query :refer :all]
            [task02.db :as db]
            ))


(db/load-initial-data)

(deftest parse-query-string-test
  (testing "parse-query-string on 'select student'"
    (let [[action tb-name & {:keys [where limit order-by joins]}]
          (parse-query-string "select student")]
      (is (= action 'select))
      (is (= tb-name "student"))
      (is (nil? where))
      (is (nil? order-by))
      (is (nil? joins))
      (is (nil? limit))))

  (testing "parse-query-string on 'select student where id = 10'"
    (let [[action tb-name & {:keys [where limit order-by joins]}]
          (parse-query-string "select student where id = 10")]
      (is (= action 'select))
      (is (= tb-name "student"))
      (is (fn? where))
      (is (nil? order-by))
      (is (nil? joins))
      (is (nil? limit))))

  (testing "parse-query-string on 'select student where id = 10 order by year limit 5 join subject on id = sid'"
    (let [[action tb-name & {:keys [where limit order-by joins]}]
          (parse-query-string "select student where id = 10 order by year limit 5 join subject on id = sid")]
      (is (= action 'select))
      (is (= tb-name "student"))
      (is (fn? where))
      (is (= order-by :year))
      (is (= limit 5))
      (is (= joins [[:id "subject" :sid]]))))

  (testing "parse-query-string returns nil on invalid query"
    (is (nil? (parse-query-string "werfwefw")))
    (is (nil? (parse-query-string "select student where")))
    (is (nil? (parse-query-string "select student limit 2 where id = 10"))))

  (testing "parse-query-string on update query"
    (let [[action tbl-name update-args & {:keys [where]}] (parse-query-string "update student set id = 10 name = \"20\" where id = 10")]
      (is (= action 'update))
      (is (= tbl-name "student"))
      (is (= update-args {:name "20", :id 10}))
      (is (where {:id 10}))
      (is (false? (where {:id 1})))))

  (testing "parse-query-string on delete query"
    (let [[action tbl-name & {:keys [where]}] (parse-query-string "delete student where name = \"hello\"")]
      (is (= action 'delete))
      (is (= tbl-name "student"))
      (is (where {:name "hello"}))
      (is (false? (where {:name "world"})))))

  (testing "parse-query-string on insert query"
    (let [[action tbl-name values] (parse-query-string "insert into student values a = \"b\" c = \"d\"")]
      (is (= action 'insert))
      (is (= tbl-name "student"))
      (is (= values {:c "d", :a "b"}))))

  (testing "parse-query-string on create table query"
    (is (= '(create-table "group") (parse-query-string "create table group"))))

  (testing "parse-query-string on drop table query"
    (is (= '(drop-table "group") (parse-query-string "drop table group")))))


(deftest perform-query-test
  (testing "perform-query"
      (is (= (perform-query "select student where year = 1997")
             '({:year 1997, :surname "Petrov", :id 2})))
      (is (= (perform-query "select student where year = 1111")
             '()))))
