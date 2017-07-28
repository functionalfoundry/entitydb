(ns workflo.entitydb.util.operations
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]))


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


(s/fdef remove-entity-or-ref-at-path
  :args (s/cat :db ::specs.v1/entitydb
               :path ::entity-path)
  :fn   (fn [{:keys [args ret]}]
          (let [path (get args :path)]
            (not (contains? (get-in ret (butlast path)) (last path)))))
  :ret  ::specs.v1/entitydb)


(defn- remove-entity-or-ref-at-path
  [db path]
  (let [db-without-entity (update-in db (butlast path) dissoc (last path))
        entity-map        (get-in db-without-entity (butlast path))]
    (if (or (nil? entity-map)
            (empty? entity-map))
      (update-in db-without-entity (butlast (butlast path)) dissoc (last (butlast path)))
      db-without-entity)))


(s/fdef remove-at-path-if-ref
  :args (s/cat :db ::specs.v1/entitydb
               :path ::entity-path)
  :ret  ::specs.v1/entitydb)


(defn- remove-at-path-if-ref
  [db path]
  (cond-> db
    (entities/ref? (get-in db path)) (remove-entity-or-ref-at-path path)))


(s/fdef add-entity
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name keyword?
               :entity ::specs.v1/entity)
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (= (get-in out-db path)
               (entities/refify-entity entity))))
  :ret ::specs.v1/entitydb)


(defn ^:export add-entity
  [db entity-name entity]
  (->> entity
       (entities/refify-entity)
       (assoc-in db (entity-path entity entity-name))))

(s/fdef add-entities
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name keyword?
               :entities ::specs.v1/entities)
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
  [db entity-name entities]
  (reduce #(add-entity %1 entity-name %2) db entities))


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


(s/fdef update-entity
  :args (s/cat :db ::specs.v1/entitydb
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
  ([db entity-name entity]
   (update-entity db entity-name entity default-merge))
  ([db entity-name entity merge-fn]
   (let [path (entity-path entity entity-name)]
     (-> db
         (update-in path (fn [entity-1 entity-2]
                           (-> (merge-fn entity-1 entity-2)
                               (entities/refify-entity)))
                    entity)
         (remove-at-path-if-ref path)))))


(s/fdef remove-entity-or-ref
  :args (s/cat :db ::specs.v1/entitydb
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
  [db entity-name entity-or-ref]
  (let [path (entity-path entity-or-ref entity-name)]
    (if (contains? (get-in db (butlast path)) (last path))
      (remove-entity-or-ref-at-path db path)
      db)))


(s/fdef remove-entity
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name keyword?
               :entity ::specs.v1/entity)
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (not (contains? (get-in out-db (butlast path)) (last path)))))
  :ret ::specs.v1/entitydb)


(defn ^:export remove-entity
  [db entity-name entity]
  (remove-entity-or-ref db entity-name entity))


(s/fdef remove-entity-by-ref
  :args (s/cat :db ::specs.v1/entitydb
               :ref ::specs.v1/ref)
  :fn   (fn [{:keys [args ret]}]
          (not-any? (fn [[entity-name entities]]
                      (contains? entities (get-in args [:ref :workflo/id])))
                    (get ret :workflo.entitydb.v1/data)))
  :ret  ::specs.v1/entitydb)


(defn ^:export remove-entity-by-ref
  [db ref]
  (let [entity-names (->> (get db :workflo.entitydb.v1/data) (keys))]
    (reduce (fn [db entity-name]
              (remove-entity-or-ref db entity-name ref))
            db entity-names)))


(s/fdef remove-entities
  :args (s/cat :db ::specs.v1/entitydb
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
  [db entity-name entities]
  (reduce #(remove-entity %1 entity-name %2) db entities))
