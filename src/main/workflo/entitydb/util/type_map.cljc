(ns workflo.entitydb.util.type-map
  (:require [clojure.spec.alpha :as s]
            [workflo.macros.entity :refer [memoized-entity-attrs
                                           registered-entities]]
            [workflo.entitydb.specs.v1 :as specs.v1]))


;;;; Generate a type map


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


(s/fdef type-map-from-registered-entities
  :args nil?
  :ret ::specs.v1/type-map)


(defn ^:export type-map-from-registered-entities
  []
  (let [attrs-by-entity  (attrs-by-entity (vals (registered-entities)))
        all-attrs        (into #{} cat (vals attrs-by-entity))
        entities-by-attr (into {} (map (fn [attr]
                                         [attr (entities-for-attr attrs-by-entity attr)]))
                               all-attrs)]
    (into {} (keep (fn [[attr entity-names]]
                     (when (= 1 (count entity-names))
                       [attr (first entity-names)])))
          entities-by-attr)))
