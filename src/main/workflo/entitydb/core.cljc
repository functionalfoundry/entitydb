(ns workflo.entitydb.core
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.macros.entity :refer [memoized-entity-attrs
                                           registered-entities]]
            [workflo.macros.entity.schema :refer [non-persistent-key?]]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]
            [workflo.entitydb.util.identity :as identity]
            [workflo.entitydb.util.indexes :as indexes]
            [workflo.entitydb.util.operations :as ops]
            [workflo.entitydb.util.schema :as schema]))


;;;; Aliases for internal utilities to make them part of the core API


(def ^:export make-id
  "Alias for `workflo.entitydb.util.identity/make-id`."
  identity/make-id)


(def ^:export entity-name
  "Alias for `workflo.entitydb.util.entities/entity-name`."
  entities/entity-name)


(def ^:export indexed-attributes-from-registered-entities
  "Alias for `workflo.entitydb.util.schema/indexed-attributes-from-registered-entities`."
  schema/indexed-attributes-from-registered-entities)


(def ^:export type-map-from-registered-entities
  "Alias for `workflo.entitydb.util.schema/type-map-from-registered-entities`."
  schema/type-map-from-registered-entities)


(def ^:export db-config-from-registered-entities
  "Alias for `workflo.entitydb.util.schema/db-config-from-registered-entities`."
  schema/db-config-from-registered-entities)


;;;; Create a db


(s/fdef empty-db
  :args nil?
  :ret ::specs.v1/entitydb)


(defn ^:export empty-db
  "Creates an empty entitydb."
  []
  {:workflo.entitydb.v1/data {}
   :workflo.entitydb.v1/indexes {}})


(s/fdef db-from-entities
  :args (s/cat :db-config ::specs.v1/db-config
               :entities ::specs.v1/entities)
  :ret  ::specs.v1/entitydb)


(defn ^:export db-from-entities
  "Creates an entitydb from a collection of entities."
  [db-config entities]
  (let [type-map (get db-config :type-map)]
    (as-> (empty-db) db
      ;; Populate the db
      (reduce (fn [db entity]
                (if-some [entity-name (entities/entity-name entity type-map)]
                  (assoc-in db [:workflo.entitydb.v1/data entity-name
                                (get entity :workflo/id)] entity)
                  db))
              db entities)

      ;; Recreate indexes
      (indexes/recreate-indexes db db-config))))


;;;; Validating dbs


(defn ^:export valid-db?
  [db]
  (s/valid? ::specs.v1/entitydb db))


;;;; Persisting a db


(s/fdef persistable-entity
  :args (s/cat :entity ::specs.v1/entity
               :attribute-names ::specs.v1/entity-attribute-names)
  :ret  ::specs.v1/entity)


(defn ^:export persistable-entity
  [entity attribute-names]
  (reduce (fn [entity-out [k v]]
            (cond-> entity-out
              (some #{k} attribute-names) (assoc k v)))
          {} entity))


(s/fdef persistable-entity-map
  :args (s/cat :entity-map ::specs.v1/entity-map
               :attribute-names ::specs.v1/entity-attribute-names)
  :ret ::specs.v1/entity-map)


(defn ^:export persistable-entity-map
  [entity-map attribute-names]
  (reduce (fn [map-out [entity-id entity]]
            (assoc map-out entity-id (persistable-entity entity attribute-names)))
          {} entity-map))


(s/fdef persistable-db
  :args (s/cat :db ::specs.v1/entitydb
               :entity-names (s/coll-of ::specs.v1/entity-name :min-count 1)
               :attribute-names ::specs.v1/entity-attribute-names)
  :ret ::specs.v1/entitydb)


(defn ^:export persistable-db
  [db entity-names attribute-names]
  {:workflo.entitydb.v1/data
   (reduce (fn [db-out [entity-name entity-map]]
             (cond-> db-out
               (some #{entity-name} entity-names)
               (assoc entity-name (persistable-entity-map entity-map attribute-names))))
           {} (get db :workflo.entitydb.v1/data))})


(defn ^:export persistable-db-for-registered-entities
  [db]
  (let [entities        (remove (comp (fn [hints]
                                        (some #{:non-persistent} hints))
                                      :hints)
                                (vals (registered-entities)))
        entity-names    (map (comp keyword :name) entities)
        attribute-names (into #{} (comp (mapcat memoized-entity-attrs)
                                        (remove non-persistent-key?))
                              entities)]
    (persistable-db db entity-names attribute-names)))


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


(defn ^:export entity-map
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


(defn ^:export get-by-id
  [db entity-name id]
  (get-in db [:workflo.entitydb.v1/data entity-name id]))


;;;; Merge dbs


(s/fdef merge-dbs
  :args (s/cat :db1 ::specs.v1/entitydb
               :db2 ::specs.v1/entitydb
               :db-config ::specs.v1/db-config)
  :ret ::specs.v1/entitydb)


(defn ^:export merge-dbs
  [db1 db2 db-config]
  (-> db1
      (update :workflo.entitydb.v1/data
              (partial merge-with merge)
              (get db2 :workflo.entitydb.v1/data))
      (indexes/recreate-indexes db-config)))


;;;; Merge entities


(s/fdef merge-entities
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entities ::specs.v1/loose-entities
               :merge-fn (s/? (s/with-gen fn? #(gen/return (comp last vector)))))
  :ret ::specs.v1/entitydb)


(defn ^:export merge-entities
  ([db db-config entities]
   (merge-entities db db-config entities ops/default-merge))
  ([db db-config entities merge-fn]
   (let [all-entities (-> entities
                          (entities/flatten-entities)
                          (entities/dedupe-entities merge-fn))]
     (reduce (fn [db entity]
               (let [entity-name (entities/entity-name entity (get db-config :type-map))]
                 (ops/update-entity db db-config entity-name entity merge-fn)))
             db all-entities))))


;;;; Flatten entities in the database


(s/fdef flattened-data
  :args (s/cat :db ::specs.v1/entitydb)
  :ret (s/or :entity-map ::specs.v1/entity-map
             :empty-map #{{}}))


(defn ^:export flattened-data
  "Returns a flattened representation (a map of entity IDs to the
   corresponding entities) of the entitydb data."
  [db]
  (transduce (map second) merge {}
             (get db :workflo.entitydb.v1/data)))


(s/fdef flattened-entities
  :args (s/cat :db ::specs.v1/entitydb)
  :ret  ::specs.v1/entities)


(defn ^:export flattened-entities
  [db]
  (into #{} (comp (map second)
                  (mapcat vals))
        (get db :workflo.entitydb.v1/data)))
