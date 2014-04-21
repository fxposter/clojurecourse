(ns datomic
  (:require
    [datomic.api :as d]
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.set :as s])
  (:refer-clojure :exclude [import]))

(def db-url "datomic:free://localhost:4334/imdb")

(d/create-database db-url)
(def conn (d/connect db-url))
(defn db [] (d/db conn))

;; hint: сделайте extenal id из feature :id
(def schema [
  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :feature/id
   :db/valueType          :db.type/string
   :db/cardinality        :db.cardinality/one
   :db/unique             :db.unique/identity}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :feature/type
   :db/valueType          :db.type/keyword
   :db/cardinality        :db.cardinality/one
   :db/index              true}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :feature/title
   :db/valueType          :db.type/string
   :db/cardinality        :db.cardinality/one
   :db/index              true}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :feature/year
   :db/valueType          :db.type/long
   :db/cardinality        :db.cardinality/one}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :series/endyear
   :db/valueType          :db.type/long
   :db/cardinality        :db.cardinality/one}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :episode/number
   :db/valueType          :db.type/long
   :db/cardinality        :db.cardinality/one}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :episode/season
   :db/valueType          :db.type/long
   :db/cardinality        :db.cardinality/one}

  {:db/id                 (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db

   :db/ident              :series/episodes
   :db/valueType          :db.type/ref
   :db/cardinality        :db.cardinality/many}
])

(defn reset []
  (d/release conn)
  (d/delete-database db-url)
  (d/create-database db-url)
  (alter-var-root #'conn (constantly (d/connect db-url)))
  @(d/transact conn schema))

(defn find-feature-by-id [id]
  (ffirst
    (d/q '[:find ?p
           :in $ ?id
           :where [?p :feature/id ?id]]
         (db) id)))

(defn- transact [payload]
  @(d/transact conn [(into {} (remove (comp nil? second) payload))]))

(defn- import-feature [feature]
  {:db/id         (d/tempid :db.part/user)
   :feature/id    (:id feature)
   :feature/title (:title feature)
   :feature/year  (:year feature)
   :feature/type  (:type feature)})

(defn- import-series [feature]
  (merge
    (import-feature feature)
    {:series/endyear (:endyear feature)}))

(defn- import-episode [feature]
  (merge
    (import-feature feature)
    {:episode/number   (:episode feature)
     :episode/season   (:season feature)
     :series/_episodes (find-feature-by-id (:series feature))}))

;; Формат файла:
;; { :type  =>   :series | :episode | :movie | :video | :tv-movie | :videogame
;;   :id    =>   str,  unique feature id
;;   :title =>   str,  feature title. Optional for episodes
;;   :year  =>   long, year of release. For series, this is starting year of the series
;;   :endyear => long, only for series, optional. Ending year of the series. If omitted, series still airs
;;   :series  => str,  only for episode. Id of enclosing series
;;   :season  => long, only for episode, optional. Season number
;;   :episode => long, only for episode, optional. Episode number
;; }
;; hint: воспользуйтесь lookup refs чтобы ссылаться на features по внешнему :id
(defn import []
  (with-open [rdr (io/reader "features.2014.edn")]
    (doseq [line (line-seq rdr)
            :let [feature (edn/read-string line)]]
      (transact
        (case (:type feature)
          :series  (import-series feature)
          :episode (import-episode feature)
                   (import-feature feature))))))

;; Найти все пары entity указанных типов с совпадающими названиями
;; Например, фильм + игра с одинаковым title
;; Вернуть #{[id1 id2], ...}
;; hint: data patterns

(defn siblings [db type1 type2]
  (d/q '[:find ?id1 ?id2
         :in $ ?type1 ?type2
         :where
           [?p1 :feature/title ?title]
           [?p2 :feature/title ?title]
           [?p1 :feature/type ?type1]
           [?p2 :feature/type ?type2]
           [?p1 :feature/id ?id1]
           [?p2 :feature/id ?id2]]
       db type1 type2))

;; Найти сериал(ы) с самым ранним годом начала
;; Вернуть #{[id year], ...}
;; hint: aggregates

(defn- oldest-series-year [db]
  (ffirst
    (d/q '[:find (min ?year)
           :where
             [?p :feature/type :series]
             [?p :feature/year ?year]]
         db)))

(defn oldest-series [db]
  (let [year (oldest-series-year db)]
    (d/q '[:find ?id ?year
           :in $ ?year
           :where
             [?p :feature/type :series]
             [?p :feature/year ?year]
             [?p :feature/id ?id]]
         db year)))

;; Найти 3 сериала с наибольшим количеством серий в сезоне
;; Вернуть [[id season series-count], ...]
;; hint: aggregates, grouping

(defn longest-season [db]
  (take 3
    (sort
      #(> (get %1 2) (get %2 2))
      (d/q '[:find ?id ?season (count ?ep)
             :where
               [?sp :series/episodes ?ep]
               [?ep :episode/season ?season]
               [?sp :feature/id ?id]]
           db))))

;; Найти 5 самых популярных названий (:title). Названия эпизодов не учитываются
;; Вернуть [[count title], ...]
;; hint: aggregation, grouping, predicates

(defn popular-titles [db]
  (take 5
    (sort
      #(compare %2 %1)
      (d/q '[:find (count ?p) ?title
             :where
               [?p :feature/type ?type]
               [(not= ?type :episode)]
               [?p :feature/title ?title]]
           db))))
