(ns workflo.entitydb.util.operations
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]))


(defn- entity-path
  [entity entity-name]
  [:workflo.entitydb.v1/data entity-name (get entity :workflo/id)])


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


(defn add-entity
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


(defn add-entities
  [db entity-name entities]
  (reduce #(add-entity %1 entity-name %2) db entities))


(s/fdef update-entity
  :args (s/cat :db ::specs.v1/entitydb
               :entity-name keyword?
               :entity ::specs.v1/entity
               :merge-fn (s/with-gen fn? #(gen/return (comp last vector))))
  :fn   (fn [{:keys [args ret]}]
          (let [out-db      ret
                entity-name (get args :entity-name)
                entity      (get args :entity)
                path        (entity-path entity entity-name)]
            (= (get-in out-db path)
               (entities/refify-entity entity))))
  :ret ::specs.v1/entitydb)


(defn update-entity
  [db entity-name entity merge-fn]
  (->> entity
       (entities/refify-entity)
       (update-in db (entity-path entity entity-name) merge-fn)))


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


(defn remove-entity
  [db entity-name entity]
  (let [path              (entity-path entity entity-name)
        db-without-entity (update-in db (butlast path) dissoc (last path))
        entity-map        (get-in db-without-entity (butlast path))]
    (if (or (nil? entity-map) (empty? entity-map))
      (update-in db-without-entity (butlast (butlast path)) dissoc (last (butlast path)))
      db-without-entity)))


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


(defn remove-entities
  [db entity-name entities]
  (reduce #(remove-entity %1 entity-name %2) db entities))
