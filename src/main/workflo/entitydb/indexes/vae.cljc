(ns workflo.entitydb.indexes.vae
  "Value Attribute Entity/Entities index."
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [workflo.entitydb.indexes.common :as common]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.specs.indexes.common :as specs.indexes]
            [workflo.entitydb.specs.indexes.vae :as specs.vae]))


(declare add-eav)


(defn ^:export empty-index
  "Creates an empty VAE index."
  []
  {})


(s/fdef recreate
  :args (s/cat :index ::specs.vae/index
               :db-config ::specs.v1/db-config
               :data :workflo.entitydb.v1/data)
  :fn   (s/and
         ;; Only indexed attributes exist in the index
         (fn [{:keys [args ret]}]
           (let [{:keys [index data db-config]} args
                 {:keys [indexed-attributes]}   db-config]
             (set/subset? (into #{} (mapcat (comp keys second)) ret)
                          (set indexed-attributes))))

         ;; All indexed attributes of all entities are present
         (fn [{:keys [args ret]}]
           (let [{:keys [index data db-config]} args
                 {:keys [indexed-attributes]}   db-config]
             (every? (fn [[entity-name entity-map]]
                       (every? (fn [[id entity]]
                                 (every? (fn [[attr value]]
                                           (or (not (some #{attr} indexed-attributes))
                                               (some #{[entity-name id]}
                                                     (get-in ret [value attr]))))
                                         entity))
                               entity-map))
                     data))))
  :ret  ::specs.vae/index)


(defn recreate
  "Creates a fresh VAE index from entitydb data, given a collection
   of indexed attributes."
  [index db-config data]
  (letfn [(populate-from-entity [index entity-name id entity]
            (reduce (fn [index [attr value]]
                      (if (some #{attr} (get db-config :indexed-attributes))
                        (add-eav index [entity-name id] attr value)
                        index))
                    index entity))
          (populate-from-entity-map [index entity-name entity-map]
            (reduce (fn [index [id entity]]
                      (populate-from-entity index entity-name id entity))
                    index entity-map))
          (populate-from-data [index data]
            (reduce (fn [index [entity-name entity-map]]
                      (populate-from-entity-map index entity-name entity-map))
                    (empty-index) data))]
    (-> (empty-index)
        (populate-from-data data))))


(s/fdef remove-eav
  :args (s/cat :index ::specs.vae/index
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/loose-entity-attribute-value)
  :fn   (fn [{:keys [args ret]}]
          (let [{:keys [index typed-ref attr value]} args]
            (not (some #{typed-ref} (get-in ret [value attr])))))
  :ret  ::specs.vae/index)


(defn remove-eav
  [index typed-ref attr value]
  (if (and (get index value)
           (get-in index [value attr]))
    (as-> index index
      ;; Remove the typed ref from the index
      (update-in index [value attr] disj typed-ref)

      ;; Make sure we don't leave an empty set of typed refs behind
      (update index value (fn [attribute-entity-map]
                            (cond-> attribute-entity-map
                              (empty? (get attribute-entity-map attr))
                              (dissoc attr))))

      ;; Make sure we also don't leave an empty attribute-entities map
      ;; behind under the old value
      (if (empty? (get index value))
        (dissoc index value)
        index))
    index))


(s/fdef add-eav
  :args (s/cat :index ::specs.vae/index
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :value ::specs.v1/strict-entity-attribute-value)
  :fn   (fn [{:keys [args ret]}]
          (let [{:keys [index db-config typed-ref attr value]} args]
            (some #{typed-ref} (get-in ret [value attr]))))
  :ret  ::specs.vae/index)


(defn add-eav
  [index typed-ref attr value]
  (update-in index [value attr] (comp set conj) typed-ref))


(s/fdef update-eav
  :args (s/cat :index ::specs.vae/index
               :typed-ref ::specs.indexes/typed-ref
               :attr ::specs.v1/entity-attribute-name
               :old-value ::specs.v1/loose-entity-attribute-value
               :new-value ::specs.v1/strict-entity-attribute-value)
  :fn   (s/or
         :different-values
         (s/and
          ;; The old and new attribute values are different
          (fn [{:keys [args]}]
            (not= (get args :old-value)
                  (get args :new-value)))

          ;; The old VAE data is no longer present
          (fn [{:keys [args ret]}]
            (let [{:keys [index typed-ref attr old-value new-value]} args]
              (not (some #{typed-ref} (get-in ret [old-value attr])))))

          ;; The new VAE data is present
          (fn [{:keys [args ret]}]
            (let [{:keys [index typed-ref attr old-value new-value]} args]
              (some #{typed-ref} (get-in ret [new-value attr])))))

         :identical-values
         (s/and
          ;; The old and new attribute values are identical
          (fn [{:keys [args]}]
            (= (get args :old-value)
               (get args :new-value)))

          ;; The old/new VAE data is still present
          (fn [{:keys [args ret]}]
            (let [{:keys [index typed-ref attr old-value new-value]} args]
              (some #{typed-ref} (get-in ret [new-value attr]))))))
  :ret  ::specs.vae/index)


(defn update-eav
  [index typed-ref attr old-value new-value]
  (-> index
      (remove-eav typed-ref attr old-value)
      (add-eav typed-ref attr new-value)))


(s/fdef index-from-db
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config)
  :ret  ::specs.vae/index)


(defn ^:export index-from-db
  [db db-config]
  (-> (empty-index)
      (recreate db-config (get db :workflo.entitydb.v1/data))))
