(ns workflo.entitydb.specs.v1
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))


;;;; Data


(defn- entity-id-matches-entity-workflo-id?
  [[entity-id entity]]
  (= entity-id
     (get entity :workflo/id)))


(s/def ::entity-id
  (s/with-gen
    (s/and string?
           #(= (count %) 32))
    #(s/gen #{"596e2b0e7ca846ed9508775ebe6f3541"
              "596e2b0e9b814772aabf6a997273b3ed"
              "596e2b0e679e46aea7388db22ccd4b57"})))


(s/def :workflo/id
  ::entity-id)


(s/def ::entity-attribute
  keyword?)


(s/def ::entity-attribute-value
  (s/and any? #(not (nil? %))))


(s/def ::entity-attribute-value-map
  (s/map-of ::entity-attribute ::entity-attribute-value
            :min-count 2
            :gen-max 10))

(s/def ::entity
  (s/with-gen
    (s/and ::entity-attribute-value-map
           (s/keys :req [:workflo/id]))
    #(gen/fmap (fn [entity]
                 (assoc entity :workflo/id (gen/generate (s/gen ::entity-id))))
               (s/gen ::entity-attribute-value-map))))


(s/def ::entities
  (s/or :set (s/coll-of ::entity :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::entity :kind vector? :min-count 1 :gen-max 10)))


(s/def ::ref
  (s/map-of #{:workflo/id} ::entity-id :min-count 1 :max-count 1))


(s/def ::refs
  (s/or :set (s/coll-of ::ref :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::ref :kind vector? :min-count 1 :gen-max 10)))


(s/def ::entity-map
  (s/with-gen
    (s/and (s/map-of ::entity-id ::entity :min-count 1 :gen-max 10)
           (s/every entity-id-matches-entity-workflo-id?))
    #(gen/fmap (fn [entities]
                 (into {} (map (juxt :workflo/id identity)) entities))
               (s/gen ::entities))))


(s/def ::entity-name
  keyword?)


(s/def ::data
  (s/map-of ::entity-name ::entity-map :gen-max 10))


;;;; Indexes


(s/def ::indexes
  (s/map-of keyword? any? :gen-max 2))


;;;; entitydb v1


(s/def :workflo.entitydb.v1/data ::data)
(s/def :workflo.entitydb.v1/indexes ::indexes)
(s/def ::entitydb
  (s/keys :req [:workflo.entitydb.v1/data]
          :opt [:workflo.entitydb.v1/indexes]))


;;;; Schema

(s/def ::type-map
  (s/map-of keyword? ::entity-name :min-count 1))
