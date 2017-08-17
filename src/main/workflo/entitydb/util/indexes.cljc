(ns workflo.entitydb.util.indexes
  (:require [clojure.spec.alpha :as s]
            [workflo.macros.entity :refer [registered-entities]]
            [workflo.macros.entity.schema :refer [entity-schema]]
            [workflo.macros.specs.types :as types]
            [workflo.entitydb.indexes.vae :as vae]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.specs.indexes.common :as specs.indexes]
            [workflo.entitydb.specs.indexes.vae :as specs.vae]))


;;;; Supported indexes


(def +supported-indexes+
  {:vae {:empty-index vae/empty-index
         :recreate vae/recreate
         :add-eav vae/add-eav
         :update-eav vae/update-eav
         :remove-eav vae/remove-eav
         :spec ::specs.vae/index}})


;;;; Obtain indexed attributes from entities


(s/def ::attribute-schema
  (s/coll-of #{:bytes :keyword :string :boolean :long :bigint :float :double
               :bigdec :instant :uuid :enum :ref :many :unique-value
               :unique-identity :indexed :fulltext :nohistory :component}))



(s/fdef attribute-indexed?
  :args (s/cat :attribute-with-schema (s/tuple ::specs.v1/entity-attribute-name
                                               ::attribute-schema))
  :fn   (fn [{:keys [args ret]}]
          (if (some #{:indexed} (second (get args :attribute-with-schema)))
            (= ret true)
            (= ret false)))
  :ret  boolean?)


(defn- attribute-indexed?
  [[attribute-name attribute-schema]]
  (boolean (some #{:indexed} attribute-schema)))


(defn- indexed-attributes-from-entities*
  [entities]
  (into #{} (comp (mapcat entity-schema)
                  (filter attribute-indexed?)
                  (map first))
        (vals entities)))


(def ^:private indexed-attributes-from-entities
  (memoize indexed-attributes-from-entities*))


(s/fdef indexed-attributes-from-registered-entities
  :args nil?
  :ret ::specs.v1/indexed-attributes)


(defn indexed-attributes-from-registered-entities
  "Returns a collection of indexed attributes from all entities that
   are currently registered with `workflo.macros.entity/defentity`."
  []
  (indexed-attributes-from-entities (registered-entities)))


(s/fdef recreate-index
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :index-name #{:vae})
  :fn   (fn [{:keys [args ret]}]
          (let [{:keys [db index-name db-config]} args]
            (s/valid? (get-in +supported-indexes+ [index-name :spec])
                      (get-in ret [:workflo.entitydb.v1/indexes index-name]))))
  :ret  ::specs.v1/entitydb)


(defn recreate-index
  [db db-config index-name]
  (if-some [impl (get +supported-indexes+ index-name)]
    (let [data (get db :workflo.entitydb.v1/data)]
      (update-in db [:workflo.entitydb.v1/indexes index-name]
                 (fn [index]
                   (let [ctor  (get impl :empty-index)
                         index (or index (ctor))
                         f     (get impl :recreate)]
                     (f index db-config data)))))
    db))


(s/fdef recreate-indexes
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config)
  :fn   (fn [{:keys [args ret]}]
          (let [{:keys [db]} args]
            (every? (fn [index-name]
                      (s/valid? (get-in +supported-indexes+ [index-name :spec])
                                (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                    (keys +supported-indexes+))))
  :ret  ::specs.v1/entitydb)


(defn recreate-indexes
  [db db-config]
  (reduce (fn [db index-name]
            (recreate-index db db-config index-name))
          db (keys +supported-indexes+)))


(s/fdef add-eav-to-index
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :index-name #{:vae}
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/strict-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is not indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; As a result, the index remains unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                 (get-in ret [:workflo.entitydb.v1/indexes index-name])))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; The resulting index is valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db db-config index-name attr]} args]
              (cond
                (some #{attr} (get db-config :indexed-attributes))
                (s/valid? (get-in +supported-indexes+ [index-name :spec])
                          (get-in ret [:workflo.entitydb.v1/indexes index-name])))))))
  :ret  ::specs.v1/entitydb)


(defn add-eav-to-index
  [db db-config index-name typed-ref attr value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (if-some [impl (get +supported-indexes+ index-name)]
      (update-in db [:workflo.entitydb.v1/indexes index-name]
                 (fn [index]
                   (let [ctor  (get impl :empty-index)
                         index (or index (ctor))
                         f     (get impl :add-eav)]
                     (f index typed-ref attr value))))
      db)
    db))


(s/fdef add-eav-to-indexes
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/strict-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; All indexes remain unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                           (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+)))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; All indexes are valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (s/valid? (get-in +supported-indexes+ [index-name :spec])
                                  (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+))))))
  :ret  ::specs.v1/entitydb)


(defn add-eav-to-indexes
  [db db-config typed-ref attr value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (reduce (fn [db index-name]
              (add-eav-to-index db db-config index-name typed-ref attr value))
            db (keys +supported-indexes+))
    db))


(s/fdef update-eav-in-index
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :index-name #{:vae}
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :old-value ::specs.v1/strict-entity-attribute-value
               :new-value ::specs.v1/strict-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is not indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; As a result, the index remains unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                 (get-in ret [:workflo.entitydb.v1/indexes index-name])))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; The resulting index is valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db db-config index-name attr]} args]
              (cond
                (some #{attr} (get db-config :indexed-attributes))
                (s/valid? (get-in +supported-indexes+ [index-name :spec])
                          (get-in ret [:workflo.entitydb.v1/indexes index-name])))))))
  :ret  ::specs.v1/entitydb)


(defn update-eav-in-index
  [db db-config index-name typed-ref attr old-value new-value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (if-some [impl (get +supported-indexes+ index-name)]
      (update-in db [:workflo.entitydb.v1/indexes index-name]
                 (fn [index]
                   (let [ctor  (get impl :empty-index)
                         index (or index (ctor))
                         f     (get impl :update-eav)]
                     (f index typed-ref attr old-value new-value))))
      db)
    db))


(s/fdef update-eav-in-indexes
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :old-value ::specs.v1/strict-entity-attribute-value
               :new-value ::specs.v1/strict-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; All indexes remain unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                           (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+)))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; All indexes are valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (s/valid? (get-in +supported-indexes+ [index-name :spec])
                                  (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+))))))
  :ret  ::specs.v1/entitydb)


(defn update-eav-in-indexes
  [db db-config typed-ref attr old-value new-value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (reduce (fn [db index-name]
              (update-eav-in-index db db-config index-name typed-ref attr old-value new-value))
            db (keys +supported-indexes+))
    db))


(s/fdef remove-eav-from-index
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :index-name #{:vae}
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/loose-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is not indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; As a result, the index remains unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                 (get-in ret [:workflo.entitydb.v1/indexes index-name])))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; The resulting index is valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db db-config index-name attr]} args]
              (cond
                (some #{attr} (get db-config :indexed-attributes))
                (s/valid? (get-in +supported-indexes+ [index-name :spec])
                          (get-in ret [:workflo.entitydb.v1/indexes index-name])))))))
  :ret  ::specs.v1/entitydb)


(defn remove-eav-from-index
  [db db-config index-name typed-ref attr value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (if-some [impl (get +supported-indexes+ index-name)]
      (update-in db [:workflo.entitydb.v1/indexes index-name]
                 (fn [index]
                   (let [ctor  (get impl :empty-index)
                         index (or index (ctor))
                         f     (get impl :remove-eav)]
                     (f index typed-ref attr value))))
      db)
    db))


(s/fdef remove-eav-from-indexes
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/loose-entity-attribute-value)
  :fn   (s/or
         :attribute-not-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (not (some #{attr} (get db-config :indexed-attributes)))))

          ;; All indexes remain unchanged
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (= (get-in db [:workflo.entitydb.v1/indexes index-name])
                           (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+)))))

         :attribute-indexed
         (s/and
          ;; The attribute is indexed
          (fn [{:keys [args ret]}]
            (let [{:keys [db-config attr]} args]
              (some #{attr} (get db-config :indexed-attributes))))

          ;; All indexes are valid
          (fn [{:keys [args ret]}]
            (let [{:keys [db index-name]} args]
              (every? (fn [index-name]
                        (s/valid? (get-in +supported-indexes+ [index-name :spec])
                                  (get-in ret [:workflo.entitydb.v1/indexes index-name])))
                      (keys +supported-indexes+))))))
  :ret  ::specs.v1/entitydb)


(defn remove-eav-from-indexes
  [db db-config typed-ref attr value]
  (if (some #{attr} (get db-config :indexed-attributes))
    (reduce (fn [db index-name]
              (remove-eav-from-index db db-config index-name typed-ref attr value))
            db (keys +supported-indexes+))
    db))
