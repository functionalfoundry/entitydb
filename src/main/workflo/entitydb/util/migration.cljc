(ns workflo.entitydb.util.migration
  (:require [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.entities :as entities]
            [workflo.entitydb.util.indexes :as indexes]
            [workflo.entitydb.util.operations :as ops]))


(defn ^:export migrate [db db-config migration type-map]
  (letfn [(migrate-reference-attribute [v from-id to-id]
            (cond
              (entities/refs? v) (into (if (set? v) #{} [])
                                       (map (fn [{:keys [workflo/id] :as ref}]
                                              (cond-> ref
                                                (= id from-id)
                                                (assoc :workflo/id to-id))))
                                       v)

              (entities/ref? v)  (cond-> v
                                   (= (get v :workflo/id) from-id)
                                   (assoc v :workflo/id to-id))

              :else v))
          (migrate-entity-references [entity from-id to-id]
            (reduce (fn [entity k]
                      (update entity k
                              migrate-reference-attribute
                              from-id to-id))
                   entity
                   (keys entity)))
          (migrate-references-1 [entity-db from-id to-id]
            (reduce (fn [entity-db id]
                      (update entity-db id
                              migrate-entity-references
                              from-id to-id))
                    entity-db
                    (keys entity-db)))
          (migrate-references [db from-id to-id]
            (reduce (fn [db entity-name]
                      (update db entity-name
                              migrate-references-1
                              from-id to-id))
                    db
                    (keys db)))
          (replace-entity [db old-entity new-entity type-map]
            (-> db
                (migrate-references (get old-entity :workflo/id)
                                    (get new-entity :workflo/id))
                (ops/remove-entity db-config old-entity type-map)
                (ops/add-entity db-config (entities/entity-name new-entity type-map) new-entity)))
          (migration-step [db step]
            (case (first step)
              :update  (let [[_ entity] step]
                         (if-some [entity-name (entities/entity-name entity type-map)]
                           (ops/update-entity db db-config entity-name entity ops/default-merge)
                           (throw (ex-info "Failed to update unrecognizable entity"
                                           {:entity entity}))))
              :replace (let [[_ old-entity new-entity] step]
                         (replace-entity db old-entity new-entity type-map))
              :add     (let [[_ entity] step]
                         (if-some [entity-name (entities/entity-name entity type-map)]
                           (ops/add-entity db db-config entity-name entity)
                           (throw (ex-info "Failed to add unrecognizable entity"
                                           {:entity entity}))))
              :remove  (let [[_ entity] step]
                         (if-some [entity-name (entities/entity-name entity type-map)]
                           (ops/remove-entity db db-config entity-name entity)
                           (throw (ex-info "Failed to remove unrecognizable entity"
                                           {:entity entity}))))))]
    (as-> db db'
      ;; Migrate data in the entitydb
      (reduce migration-step db' migration)

      ;; Recreate indexes. Note: this is only necessary as there's no good
      ;; solution to updating indexes when migrating references yet. We're
      ;; doing this to keep indexes in sync but it's a little slow.
      (indexes/recreate-indexes db' db-config))))
