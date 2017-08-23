(ns workflo.entitydb.util.schema
  (:require [clojure.spec.alpha :as s]
            [workflo.macros.entity :refer [entity-refs
                                           memoized-entity-attrs
                                           registered-entities]]
            [workflo.macros.entity.schema :refer [entity-schema]]
            [workflo.entitydb.specs.v1 :as specs.v1]))


;;;; Type map


(defn- attrs-by-entity
  [entities]
  (into {} (map (juxt (comp keyword :name)
                      memoized-entity-attrs))
        entities))


(defn- entities-for-attr
  [attrs-by-entity attr]
  (into #{} (comp (filter (fn [[entity-name attrs]]
                            (some #{attr} attrs)))
                  (map first))
        attrs-by-entity))


(defn- type-map-from-entities*
  [entities]
  (let [attrs-by-entity  (attrs-by-entity (vals entities))
        all-attrs        (into #{} cat (vals attrs-by-entity))
        entities-by-attr (into {} (map (fn [attr]
                                         [attr (entities-for-attr attrs-by-entity attr)]))
                               all-attrs)]
    (into {} (keep (fn [[attr entity-names]]
                     (when (= 1 (count entity-names))
                       [attr (first entity-names)])))
          entities-by-attr)))


(def ^:private type-map-from-entities (memoize type-map-from-entities*))


(s/fdef type-map-from-registered-entities
  :args nil?
  :ret ::specs.v1/type-map)


(defn ^:export type-map-from-registered-entities
  "Returns a type map from all entities that are currently
   registered with `workflo.macros.entity/defentity`."
  []
  (type-map-from-entities (registered-entities)))


;;;; Indexed attributes


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


;;;; Reference attributes


(s/fdef entity-ref-attributes-from-entity-name
  :args (s/cat :entity-name ::specs.v1/entity-name)
  :ret  ::specs.v1/entity-ref-attributes)


(defn- entity-ref-attributes-from-entity-name
  [entity-name]
  (let [macros-entity-name (symbol (subs (str entity-name) 1))]
    (into {} (map (fn [[attr ref-info]]
                    [attr {:source-entity entity-name
                           :target-entity (keyword (get ref-info :entity))
                           :many? (get ref-info :many? false)}]))
          (entity-refs macros-entity-name))))


(defn- entity-ref-attributes-from-entities*
  [entities]
  (into {} (mapcat (comp entity-ref-attributes-from-entity-name
                         keyword
                         first))
        entities))


(def ^:private entity-ref-attributes-from-entities
  (memoize entity-ref-attributes-from-entities*))


(s/fdef entity-ref-attributes-from-registered-entities
  :args nil?
  :ret  ::specs.v1/entity-ref-attributes)


(defn ^:export entity-ref-attributes-from-registered-entities
  "Returns the entity reference attributes defined by the `entities`.
   The result is a map of attribute name to reference info."
  []
  (entity-ref-attributes-from-entities (registered-entities)))


;;;; DB config


(s/fdef db-config-from-registered-entities
  :args nil?
  :ret ::specs.v1/db-config)


(defn db-config-from-registered-entities
  []
  {:type-map (type-map-from-registered-entities)
   :indexed-attributes (indexed-attributes-from-registered-entities)
   :entity-ref-attributes (entity-ref-attributes-from-registered-entities)})
