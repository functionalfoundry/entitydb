(ns workflo.entitydb.util.operations
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.entitydb.indexes.common :as indexes.common]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]
            [workflo.entitydb.util.indexes :as indexes]))


;;;; Helpers


(s/def ::entity-path
  (s/tuple #{:workflo.entitydb.v1/data}
           ::specs.v1/entity-name
           ::specs.v1/entity-id))


(s/fdef entity-path
  :args (s/cat :entity-or-ref (s/or :entity ::specs.v1/loose-entity
                                    :ref ::specs.v1/ref)
               :entity-name ::specs.v1/entity-name)
  :ret  ::entity-path)


(defn- entity-path
  [entity entity-name]
  [:workflo.entitydb.v1/data entity-name (get entity :workflo/id)])


;;;; Indexes


(s/fdef remove-entity-from-indexes
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name ::specs.v1/entity-name
               :path ::entity-path)
  :ret ::specs.v1/entitydb)


(defn- remove-entity-from-indexes
  [db db-config entity-name path]
  (let [entity    (get-in db (conj path :workflo/id))
        typed-ref [entity-name (get entity :workflo/id)]]
    (reduce (fn [db [attr value]]
              (indexes/remove-eav-from-indexes db db-config typed-ref attr value))
            db entity)))


(defn- collect-eav-updates-from-entity
  [typed-ref old-entity new-entity]
  (reduce (fn [eav-updates [attr new-value]]
            (cond
              ;; The attribute is being removed from the entity:
              ;; Remove the EAV triplet from indexes
              (nil? new-value)
              (let [old-value (get old-entity attr)]
                (conj eav-updates [:removed typed-ref attr old-value]))

              ;; The attribute is being added to the entity:
              ;; Add the EAV triplet to indexes
              (not (contains? old-entity attr))
              (conj eav-updates [:added typed-ref attr new-value])

              ;; The attribute value is being changed in the entity:
              ;; Update the EAV triplet in indexes
              :else
              (let [old-value (get old-entity attr)]
                (conj eav-updates [:updated typed-ref attr old-value new-value]))))
          [] new-entity))


(defn- update-entity-in-indexes
  "Updates all indexes in the db to reflect the attribute/value changes
   made to an entity."
  [db db-config entity-name old-entity new-entity]
  (let [typed-ref   [entity-name (get new-entity :workflo/id)]
        eav-updates (collect-eav-updates-from-entity typed-ref old-entity new-entity)]
    (reduce (fn [db update]
              (let [[update-type typed-ref attr value-1 value-2] update]
                (case update-type
                  :removed (indexes/remove-eav-from-indexes
                            db db-config typed-ref attr value-1)
                  :added   (indexes/add-eav-to-indexes
                            db db-config typed-ref attr value-1)
                  :updated (indexes/update-eav-in-indexes
                            db db-config typed-ref attr value-1 value-2))))
            db eav-updates)))


;;;; Entity merging


(s/fdef default-merge
  :args (s/cat :entity-1 (s/or :entity ::specs.v1/entity
                               :nil nil?)
               :entity-2 ::specs.v1/loose-entity)
  :ret  (s/or :entity ::specs.v1/loose-entity
              :ref ::specs.v1/ref))


(defn ^:export default-merge
  [entity-1 entity-2]
  (reduce (fn [entity-1 [k v]]
            (if (nil? v)
              (dissoc entity-1 k)
              (assoc entity-1 k v)))
          entity-1 entity-2))


;;;; Entity removal


(s/fdef remove-entity-or-ref-at-path
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name ::specs.v1/entity-name
               :path ::entity-path)
  :fn   (fn [{:keys [args ret]}]
          (let [path (get args :path)]
            (not (contains? (get-in ret (butlast path)) (last path)))))
  :ret  ::specs.v1/entitydb)


(defn- remove-entity-or-ref-at-path
  "Removes the entity (or ref) at the given path from the db."
  [db db-config entity-name path]
  (let [db-without-entity (-> db
                              (remove-entity-from-indexes db-config entity-name path)
                              (update-in (butlast path) dissoc (last path)))
        entity-map        (get-in db-without-entity (butlast path))]
    (if (or (nil? entity-map)
            (empty? entity-map))
      (update-in db-without-entity (butlast (butlast path)) dissoc (last (butlast path)))
      db-without-entity)))


(s/fdef remove-at-path-if-ref
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name ::specs.v1/entity-name
               :path ::entity-path)
  :ret  ::specs.v1/entitydb)


(defn- remove-at-path-if-ref
  "Removes the entity at the given path from the db if it no longer
   has any attributes set (which is the same as being a ref)."
  [db db-config entity-name path]
  (cond-> db
    (entities/ref? (get-in db path))
    (remove-entity-or-ref-at-path db-config entity-name path)))


(s/fdef remove-entity-or-ref
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name ::specs.v1/entity-name
               :entity-or-ref (s/or :entity ::specs.v1/entity
                                    :ref ::specs.v1/ref))
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity-or-ref (second (get args :entity-or-ref))
                path        (entity-path entity-or-ref entity-name)]
            (not (contains? (get-in out-db (butlast path)) (last path)))))
  :ret  ::specs.v1/entitydb)


(defn- remove-entity-or-ref
  "Removes the given entity or ref from the db."
  [db db-config entity-name entity-or-ref]
  (let [path (entity-path entity-or-ref entity-name)]
    (if (contains? (get-in db (butlast path)) (last path))
      (remove-entity-or-ref-at-path db db-config entity-name path)
      db)))


(s/fdef remove-entity
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name keyword?
               :entity ::specs.v1/entity)
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (not (contains? (get-in out-db (butlast path)) (last path)))))
  :ret  ::specs.v1/entitydb)


(defn ^:export remove-entity
  "Removes the given entity from the db."
  [db db-config entity-name entity]
  (remove-entity-or-ref db db-config entity-name entity))


(s/fdef remove-entity-by-ref
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :ref ::specs.v1/ref)
  :fn   (fn [{:keys [args ret]}]
          (not-any? (fn [[entity-name entities]]
                      (contains? entities (get-in args [:ref :workflo/id])))
                    (get ret :workflo.entitydb.v1/data)))
  :ret  ::specs.v1/entitydb)


(defn ^:export remove-entity-by-ref
  "Removes the entity from the db that corresponds to the given ref.
   *Note: This is a slow operation, as the ref lacks type information and
   thus this function has to check all entity maps to find the corresponding
   entity.*"
  [db db-config ref]
  (let [entity-names (->> (get db :workflo.entitydb.v1/data) (keys))]
    (reduce (fn [db entity-name]
              (remove-entity-or-ref db db-config entity-name ref))
            db entity-names)))


(s/fdef remove-entities
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name keyword?
               :entity ::specs.v1/entity)
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entities    (get-in args [:entities 1])]
            (not-any? (fn [entity]
                        (let [path (entity-path entity entity-name)]
                          (contains? (get-in out-db (butlast path))
                                     (last path))))
                      entities)))
  :ret ::specs.v1/entitydb)


(defn ^:export remove-entities
  "Removes all given entities from the db."
  [db db-config entity-name entities]
  (reduce #(remove-entity %1 db-config entity-name %2) db entities))


;;;; Entity updates


(defn- update-entity-at-path
  [db db-config entity-name entity path merge-fn]
  (let [old-entity (get-in db path)
        new-entity (-> (merge-fn old-entity entity)
                       (entities/refify-entity))]
    (-> db
        (assoc-in path new-entity)
        (update-entity-in-indexes db-config entity-name old-entity new-entity))))


(s/fdef update-entity
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name keyword?
               :entity ::specs.v1/loose-entity
               :merge-fn (s/? (s/with-gen fn? #(gen/return default-merge))))
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (= (get-in out-db path)
               (entities/refify-entity entity))))
  :ret ::specs.v1/entitydb)


(defn ^:export update-entity
  ([db db-config entity-name entity]
   (update-entity db db-config entity-name entity default-merge))
  ([db db-config entity-name entity merge-fn]
   (let [path (entity-path entity entity-name)]
     (-> db
         (update-entity-at-path db-config entity-name entity path merge-fn)
         (remove-at-path-if-ref db-config entity-name path)))))


;;;; Entity addition


(s/fdef add-entity
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name keyword?
               :entity ::specs.v1/entity
               :merge-fn (s/? (s/with-gen fn? #(gen/return default-merge))))
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (= (get-in out-db path)
               (entities/refify-entity entity))))
  :ret ::specs.v1/entitydb)


(defn ^:export add-entity
  "Adds the given entity to the db, or updates the existing instance
   if the entity already exists in the db."
  ([db db-config entity-name entity]
   (add-entity db db-config entity-name entity default-merge))
  ([db db-config entity-name entity merge-fn]
   (let [path       (entity-path entity entity-name)
         old-entity (get-in db path)]
     (if (nil? old-entity)
       ;; Add the entity to the db
       (let [new-entity (entities/refify-entity entity)]
         (-> db
             (assoc-in path new-entity)
             (update-entity-in-indexes db-config entity-name old-entity new-entity)))
       ;; Update the existing entity in the db
       (update-entity-at-path db db-config entity-name entity path merge-fn)))))


(s/fdef add-entities
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :entity-name keyword?
               :entities ::specs.v1/entities
               :merge-fn (s/? (s/with-gen fn? #(gen/return default-merge))))
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entities    (->> (get-in args [:entities 1])
                                 (group-by :workflo/id))]
            (every? (fn [[id entities]]
                      (some (fn [entity]
                              (= (get-in out-db (entity-path entity entity-name))
                                 (entities/refify-entity entity)))
                            entities))
                    entities)))
  :ret ::specs.v1/entitydb)


(defn ^:export add-entities
  ([db db-config entity-name entities]
   (add-entities db db-config entity-name entities default-merge))
  ([db db-config entity-name entities merge-fn]
   (reduce #(add-entity %1 db-config entity-name %2 merge-fn) db entities)))
