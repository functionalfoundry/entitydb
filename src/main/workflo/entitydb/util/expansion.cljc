(ns workflo.entitydb.util.expansion
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.util.entities :as entities]))


;;;; Expansions


(declare expand-refs)


(defn expand-ref
  [ref flat-db]
  (get flat-db (get ref :workflo/id)))


(defn expand-ref-or-refs
  [ref-or-refs flat-db attrs]
  (cond
    (entities/refs? ref-or-refs) (->> ref-or-refs
                                      (map #(expand-ref % flat-db))
                                      (map #(expand-refs % flat-db attrs))
                                      (into #{}))
    (entities/ref? ref-or-refs)  (-> ref-or-refs
                                     (expand-ref flat-db)
                                     (expand-refs flat-db attrs))
    :else                        ref-or-refs))


(defn expand-refs
  "Expands the refs of an entity by replacing them with the
   referenced entities. Only does this for the given attrs.
   Uses a flattened entitydb to resolve refs."
  [entity flat-db attrs]
  (letfn [(maybe-expand-ref [[attr value]]
            [attr (cond-> value
                    (some #{attr} attrs) (expand-ref-or-refs flat-db attrs))])]
    (into {} (map maybe-expand-ref) entity)))


(defn expand-entity
  [entity flat-db attrs-to-expand]
  (expand-refs entity flat-db attrs-to-expand))


(defn expand-entities
  [entities flat-db attrs-to-expand]
  (into #{} (map #(expand-entity % flat-db attrs-to-expand)) entities))
