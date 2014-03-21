(ns task02.db-test
  (:require [task02.db :refer :all]
            [task02.query :as q]
            [clojure.test :refer :all]))

(load-initial-data)

(deftest insert-test
  (load-initial-data)
  (testing "insertion..."
    (insert (get-table "student") {:id 10 :surname "Test" :year 2000})
    (let [rs (q/perform-query "select student where id = 10")]
      (is (not (empty? rs)))
      (is (= (count rs) 1))
      (is (= (:year (first rs)) 2000)))
    ))

(deftest delete-test
  (load-initial-data)
  (testing "deletion..."
    (delete (get-table "student") :where (q/make-where-function "id" "=" "1"))
    (let [rs (q/perform-query "select student where id = 1")]
      (is (empty? rs)))
    (is (= (count (q/perform-query "select student")) 2))
    ))

(deftest update-test
  (load-initial-data)
  (testing "update..."
    (let [rs (q/perform-query "select student where id = 1")]
      (is (not (empty? rs)))
      (is (= (count rs) 1))
      (is (= (:year (first rs)) 1998))
      )
    (update (get-table "student") {:year 2000} :where (q/make-where-function "id" "=" "1"))
    (let [rs (q/perform-query "select student where id = 1")]
      (is (not (empty? rs)))
      (is (= (count rs) 1))
      (is (= (:year (first rs)) 2000))
      )
    ))

(deftest create-table-test
  (testing "throws when table does not exist"
    (is (thrown? IllegalArgumentException (q/perform-query "select group"))))

  (testing "creates table with specified name"
    (create-table "group")
    (insert (get-table "group") {:id 42 :name "Example"})
    (is (= (q/perform-query "select group") [{:id 42 :name "Example"}]))))

(deftest drop-table-test
  (testing "drops table with specified name"
    (create-table "group")
    (drop-table "group")
    (is (thrown? IllegalArgumentException (q/perform-query "select group")))))
