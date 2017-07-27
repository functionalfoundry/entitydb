(ns workflo.entitydb.specs.v1
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [workflo.macros.specs.types :as types]))


;;;; Data


(defn- entity-id-matches-entity-workflo-id?
  [[entity-id entity]]
  (= entity-id
     (get entity :workflo/id)))


(s/def ::entity-id
  (s/and ::types/id ::types/unique-identity))


(s/def ::entity-attribute-name
  (s/with-gen
    keyword?
    #(s/gen #{:team/name :team/users :team/project
              :user/name :user/email :user/address
              :project/name})))


(s/def ::strict-entity-attribute-value
  (s/and any?
         #(not (nil? %))
         #(not (s/valid? ::entity %))))


(s/def ::loose-entity-attribute-value
  (s/and any? #(not (nil? %))))


(s/def ::nilable-loose-entity-attribute-value
  (s/nilable ::loose-entity-attribute-value))


(s/def ::strict-entity-attribute-value-map
  (s/map-of ::entity-attribute-name ::strict-entity-attribute-value
            :min-count 2
            :gen-max 10))


(s/def ::loose-entity-attribute-value-map
  (s/map-of ::entity-attribute-name ::loose-entity-attribute-value
            :min-count 2
            :gen-max 10))


(s/def ::nilable-loose-entity-attribute-value-map
  (s/map-of ::entity-attribute-name ::nilable-loose-entity-attribute-value
            :min-count 2
            :gen-max 10))


(s/def ::entity-attribute-names
  (s/with-gen
    (s/and (s/coll-of ::entity-attribute-name :min-count 2)
           #(some #{:workflo/id} %))
    #(gen/fmap (fn [names]
                 (set (conj names :workflo/id)))
               (s/gen (s/coll-of ::entity-attribute-name
                                 :min-count 7
                                 :distinct true)))))

(s/def ::entity
  (s/with-gen
    (s/and ::strict-entity-attribute-value-map
           (s/keys :req [:workflo/id]))
    #(gen/fmap (fn [entity]
                 (assoc entity :workflo/id (gen/generate (s/gen ::entity-id))))
               (s/gen ::strict-entity-attribute-value-map))))


(s/def ::entities
  (s/or :set (s/coll-of ::entity :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::entity :kind vector? :min-count 1 :gen-max 10)))


(s/def ::loose-entity
  (s/with-gen
    (s/and ::loose-entity-attribute-value-map
           (s/keys :req [:workflo/id]))
    #(gen/fmap (fn [entity]
                 (assoc entity :workflo/id (gen/generate (s/gen ::entity-id))))
               (s/gen ::loose-entity-attribute-value-map))))

(s/def ::nilable-loose-entity
  (s/with-gen
    (s/and ::nilable-loose-entity-attribute-value-map
           (s/keys :req [:workflo/id]))
    #(gen/fmap (fn [entity]
                 (assoc entity :workflo/id (gen/generate (s/gen ::entity-id))))
               (s/gen ::nilable-loose-entity-attribute-value-map))))


(s/def ::loose-entities
  (s/or :set (s/coll-of ::loose-entity :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::loose-entity :kind vector? :min-count 1 :gen-max 10)))


(s/def ::nilable-loose-entities
  (s/or :set (s/coll-of ::nilable-loose-entity :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::nilable-loose-entity :kind vector? :min-count 1 :gen-max 10)))


(s/def ::refified-entity
  (s/and ::entity
         (fn [entity]
           (every? (fn [val]
                     (and (not (s/valid? ::entity val))
                          (not (s/valid? ::entities val))))
                   (vals entity)))))


(s/def ::refified-entities
  (s/or :set (s/coll-of ::refified-entity :kind set? :min-count 1 :gen-max 10)
        :vector (s/coll-of ::refified-entity :kind vector? :min-count 1 :gen-max 10)))


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
  (s/with-gen
    keyword?
    #(s/gen #{:user :team :project})))


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
  (s/with-gen
    (s/map-of ::entity-attribute-name ::entity-name)
    #(s/gen #{{:project/name :project
               :team/name :team
               :team/users :team
               :team/project :team
               :user/name :user
               :user/email :user
               :user/address :user}})))
