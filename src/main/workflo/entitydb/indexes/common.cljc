(ns workflo.entitydb.indexes.common
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.specs.indexes.common :as specs.indexes]
            [workflo.entitydb.util.entities :as entities]))


(s/fdef unchecked-typed-ref
  :args (s/cat :entity ::specs.v1/loose-entity
               :type-map ::specs.v1/type-map)
  :ret  (s/or :entity-name-unresolvable nil?
              :typed-ref ::specs.indexes/typed-ref))


(defn unchecked-typed-ref
  "Unchecked version of `checked-typed-ref`."
  [entity type-map]
  (when-some [entity-name (entities/entity-name entity type-map)]
    [entity-name (get entity :workflo/id)]))


(defn typed-ref
  "Takes an entity and a type map. Returns a so-called `typed ref` of the
   form `[<entity name (keyword)> <workflo ID>]`.

   Throws if the entity type cannot be resolved using the type map."
  [entity type-map]
  (if-let [ref (unchecked-typed-ref entity type-map)]
    ref
    (throw (ex-info "Failed to infer entity name to create typed ref for entity"
                    {:entity entity :type-map type-map}))))
