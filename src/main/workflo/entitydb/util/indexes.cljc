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


;;;; Typed refs


(s/fdef typed-ref?
  :args (s/cat :type-map ::specs.v1/type-map
               :x (s/or :typed-ref ::specs.indexes/typed-ref
                        :other any?))
  :fn   (s/or :typed-ref
              (s/and
               ;; The input is a valid typed ref given the type map
               (fn [{:keys [args]}]
                 (let [{:keys [type-map x]} args]
                   (let [[_ real-x] x]
                     (and (s/valid? ::specs.indexes/typed-ref real-x)
                          (some #{(first real-x)} (vals type-map))))))

               ;; The result is true
               (fn [{:keys [ret]}]
                 (= true ret)))

              :no-typed-ref
              (s/and
               ;; The input is not a valid typed ref given the type map
               (fn [{:keys [args]}]
                 (let [{:keys [type-map x]} args]
                   (let [[_ real-x] x]
                     (or (not (s/valid? ::specs.indexes/typed-ref real-x))
                         (not (some #{(first real-x)} (vals type-map)))))))

               ;; The result is false
               (fn [{:keys [ret]}]
                 (= false ret))))
  :ret  boolean?)


(defn typed-ref?
  [type-map x]
  (boolean
   (and (vector? x)
        (= (count x) 2)
        (keyword? (first x))
        (string? (second x))
        (some #{(first x)} (vals type-map)))))


(s/fdef typed-refs?
  :args (s/cat :type-map ::specs.v1/type-map
               :coll (s/or :typed-refs ::specs.indexes/typed-refs
                           :other (s/coll-of any?)))
  :fn   (s/or
         :typed-refs
         (s/and
          ;; The input collection is a collection of typed refs
          (fn [{:keys [args ret]}]
            (let [{:keys [type-map coll]} args]
              (and (s/valid? ::specs.indexes/typed-refs (second coll))
                   (every? #(typed-ref? type-map %) (second coll)))))

          ;; The result is true
          (fn [{:keys [args ret]}]
            (= true ret)))

         :no-typed-refs
         (s/and
          ;; The input collection is not a collection of typed refs
          (fn [{:keys [args ret]}]
            (let [{:keys [type-map coll]} args]
              (or (not (s/valid? ::specs.indexes/typed-refs (second coll)))
                  (some #(not (typed-ref? type-map %)) (second coll)))))

          ;; The result is false
          (fn [{:keys [args ret]}]
            (= false ret))))
  :ret  boolean?)


(defn typed-refs?
  [type-map coll]
  (and (not (empty? coll))
       (every? #(typed-ref? type-map %) coll)))


(s/fdef typed-ref->ref
  :args (s/cat :typed-ref ::specs.indexes/typed-ref)
  :fn   (fn [{:keys [args ret]}]
          (= (get ret :workflo/id)
             (second (get args :typed-ref))))
  :ret  ::specs.v1/ref)


(defn typed-ref->ref
  [typed-ref]
  {:workflo/id (second typed-ref)})


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
          ;; The attribute is not indexed
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
