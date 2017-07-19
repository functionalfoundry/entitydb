(ns workflo.entitydb.specs.v1
  (:require [clojure.spec.alpha :as s]))


;;;; Data


(defn entity-id-matches-entity-workflo-id?
  [entity-id entity]
  (= entity-id
     (get entity :workflo/id)))


(s/def ::entity-id
  (s/and string?
         #(= (count %) 40)))


(s/def :workflo/id
  ::entity-id)


(s/def ::entity-instance
  (s/and (s/map-of :entity-attribute ::entity-attribute-value)
         (s/keys :req [:workflo/id])))


(s/def ::entity-data
  (s/and (s/map-of ::entity-id ::entity-instance)
         (s/every entity-id-matches-entity-workflo-id?)))


(s/def ::entity-name
  keyword?)


(s/def ::data
  (s/map-of ::entity-name ::entity-data))


;;;; Indexes


(s/def ::indexes
  (s/map-of keyword? any?))


;;;; entitydb v1


(s/def :workflo.entitydb.v1/data ::data)
(s/def :workflo.entitydb.v1/indexes ::indexes)
(s/def :workflo.entitydb/v1
  (s/keys :req [:workflo.entitydb.v1/data]
          :opt [:workflo.entitydb.v1/indexes]))
