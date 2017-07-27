(ns workflo.entitydb.core
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]
            [workflo.entitydb.util.identity :as identity]
            [workflo.entitydb.util.operations :as ops]
            [workflo.entitydb.util.type-map :as type-map]))


;;;; Aliases for internal utilities to make them part of the core API


(def make-id
  "Alias for `workflo.entitydb.util.identity/make-id`."
  identity/make-id)


(def type-map-from-registered-entities
  "Alias for `workflo.entitydb.util.type-map/type-map-from-registered-entities`."
  type-map/type-map-from-registered-entities)


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


;;;; Accessing db contents


(s/fdef entity-map
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name ::specs.v1/entity-name)
  :fn   (fn [{:keys [args ret]}]
          (= (second ret)
             (get-in (get args :db)
                     [:workflo.entitydb.v1/data
                      (get args :entity-name)])))
  :ret  (s/or :name-exists ::specs.v1/entity-map
              :name-doesnt-exist nil?))


(defn entity-map
  [db entity-name]
  (get-in db [:workflo.entitydb.v1/data entity-name]))


(s/fdef get-by-id
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name ::specs.v1/entity-name
               :id ::specs.v1/entity-id)
  :fn   (fn [{:keys [args ret]}]
          (= (second ret)
             (get-in (get args :db)
                     [:workflo.entitydb.v1/data
                      (get args :entity-name)
                      (get args :id)])))
  :ret  (s/or :found ::specs.v1/entity
              :not-found nil?))


(defn get-by-id
  [db entity-name id]
  (get-in db [:workflo.entitydb.v1/data entity-name id]))


;;;; Merge dbs


(s/fdef merge-dbs
  :args (s/cat :db1 ::specs.v1/entitydb
               :db2 ::specs.v1/entitydb)
  :ret ::specs.v1/entitydb)


(defn merge-dbs
  [db1 db2]
  (update db1 :workflo.entitydb.v1/data
          (partial merge-with merge)
          (get db2 :workflo.entitydb.v1/data)))


;;;; Merge entities


(s/fdef merge-entities
  :args (s/cat :db ::specs.v1/entitydb
               :entities ::specs.v1/loose-entities
               :type-map ::specs.v1/type-map
               :merge-fn (s/? (s/with-gen fn? #(gen/return (comp last vector)))))
  :ret ::specs.v1/entitydb)


(defn merge-entities
  ([db entities type-map]
   (merge-entities db entities type-map ops/default-merge))
  ([db entities type-map merge-fn]
   (let [all-entities (-> entities
                          (entities/flatten-entities)
                          (entities/dedupe-entities merge-fn))]
     (reduce (fn [db entity]
               (let [entity-name (entities/entity-name entity type-map)]
                 (ops/update-entity db entity-name entity merge-fn)))
             db all-entities))))


;;;; Flatten entities in the database


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
