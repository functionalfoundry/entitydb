(ns workflo.entitydb.core
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.identity :as identity]
            [workflo.entitydb.util.entities :as entities]))


;;;; Aliases for internal utilities to make them part of the core API


(def make-id
  "Alias for `workflo.entitydb.util.identity/make-id`."
  identity/make-id)


;;;; Create a db


(s/fdef empty-db
  :args nil?
  :ret ::specs.v1/entitydb)


(defn empty-db
  "Creates an empty entitydb."
  []
  {:workflo.entitydb.v1/data {}
   :workflo.entitydb.v1/indexes {}})


(s/fdef db-from-entities
  :args (s/cat :entities ::specs.v1/entities
               :type-map ::specs.v1/type-map)
  :ret  ::specs.v1/entitydb)


(defn db-from-entities
  "Creates an entitydb from a collection of entities."
  [entities type-map]
  (reduce (fn [db entity]
            (if-some [entity-name (entities/entity-name entity type-map)]
              (assoc-in db [:workflo.entitydb.v1/data entity-name (get entity :workflo/id)] entity)
              db))
          (empty-db) entities))


(s/fdef merge-dbs
  :args (s/cat :db1 ::specs.v1/entitydb
               :db2 ::specs.v1/entitydb)
  :ret ::specs.v1/entitydb)


(defn merge-dbs
  [db1 db2]
  (update db1 :workflo.entitydb.v1/data
          (partial merge-with merge)
          (get db2 :workflo.entitydb.v1/data)))


(s/fdef flattened-data
  :args (s/cat :db ::specs.v1/entitydb)
  :ret (s/or :entity-map ::specs.v1/entity-map
             :empty-map #{{}}))


(defn flattened-data
  "Returns a flattened representation (a map of entity IDs to the
   corresponding entities) of the entitydb data."
  [db]
  (transduce (map second) merge {}
             (get db :workflo.entitydb.v1/data)))
