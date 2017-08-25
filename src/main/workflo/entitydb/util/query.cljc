(ns workflo.entitydb.util.query
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.specs.indexes.common :as specs.indexes]
            [workflo.entitydb.util.entities :as entities]
            [workflo.entitydb.util.indexes :as indexes]))


#?(:cljs (enable-console-print!))


;;;; Helpers


(defn- backref-attr?
  "Returns true if `attr` is a backref symbol or keyword, i.e., its
   name segment begins with an underscore."
  [attr]
  (str/starts-with? (name attr) "_"))


(defn backref-attr->forward-attr [k]
  (keyword (namespace k) (subs (name k) 1)))


;;;; Query internals


(s/fdef resolve-attribute-value-in-entity-map
  :args (s/cat :db-config ::specs.v1/db-config
               :entity-name ::specs.v1/entity-name
               :entity-map ::specs.v1/entity-map
               :attr ::specs.v1/entity-attribute-name
               :value any?)
  :ret  (s/or :matches-found ::specs.indexes/typed-refs
              :no-matches-found #{#{}}))


(defn- resolve-attribute-value-in-entity-map
  [db-config entity-name entity-map attr value]
  (let [ref-info (get-in db-config [:entity-ref-attributes attr])]
    (into #{} (keep (fn [[id entity]]
                      (let [attr-value (get entity attr)]
                        (when (or
                               ;; The attribute value and the search value are identical:
                               (= attr-value value)

                               ;; The attribute is a singular ref attribute that points
                               ;; to the input value
                               (and ref-info
                                    (not (get ref-info :many?))
                                    (if (entities/ref? value)
                                      (= attr-value value)
                                      (= attr-value {:workflo/id value})))

                               ;; The attribute is a many ref attribute that includes
                               ;; the input value
                               (and ref-info
                                    (get ref-info :many?)
                                    (if (entities/ref? value)
                                      (some #{value} attr-value)
                                      (some #{{:workflo/id value}} attr-value))))
                          [entity-name id]))))
          entity-map)))


(s/fdef resolve-ref-attribute-into-typed-refs
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :attr ::specs.v1/entity-attribute-name
               :ref-info ::specs.v1/entity-ref-info
               :source-ref ::specs.v1/ref)
  :ret  (s/or :typed-refs ::specs.indexes/typed-refs
              :empty-set #{#{}}))


(defn- resolve-ref-attribute-into-typed-refs
  [db db-config attr ref-info source-ref]
  (let [source-entity (get ref-info :source-entity)
        target-entity (get ref-info :target-entity)
        ref-or-refs   (get-in db [:workflo.entitydb.v1/data source-entity
                                  (get source-ref :workflo/id) attr])]
    (if (entities/ref? ref-or-refs)
      #{[target-entity (get ref-or-refs :workflo/id)]}
      (into #{} (map (fn [ref]
                       [target-entity (get ref :workflo/id)]))
            ref-or-refs))))


(s/fdef resolve-attribute-value-into-typed-refs
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :attr ::specs.v1/entity-attribute-name
               :value any?)
  :ret  (s/or :matches-found ::specs.indexes/typed-refs
              :no-matches-found #{#{}}))


(defn- resolve-attribute-value-into-typed-refs
  [db db-config attr value]
  (cond
    ;; The attribute is indexed, we can look up the typed refs of
    ;; entities that have the attribute set to the given value
    ;; directly using the VAE index
    (some #{attr} (get db-config :indexed-attributes))
    (or (get-in db [:workflo.entitydb.v1/indexes :vae value attr])
        #{})

    ;; The attribute is a backref attribute
    (backref-attr? attr)
    (let [forward-attr (backref-attr->forward-attr attr)
          ref-info     (get-in db-config [:entity-ref-attributes forward-attr])]
      (if (nil? ref-info)
        (throw (ex-info (str "Invalid backref attribute " attr " does not "
                             "correspond to any existing attributes")
                        {:backref-attribute attr
                         :forward-attribute forward-attr}))
        (resolve-ref-attribute-into-typed-refs db db-config
                                               forward-attr ref-info value)))

    ;; The attribute is neither indexed, nor a backref attribute.
    ;; It is however unique to a specific entity, so we can get
    ;; away with scanning only the entities in the corresponding
    ;; entity map for matches.
    (contains? (get db-config :type-map) attr)
    (let [entity-name (get-in db-config [:type-map attr])
          entity-map  (get-in db [:workflo.entitydb.v1/data entity-name])]
      (resolve-attribute-value-in-entity-map db-config entity-name
                                             entity-map attr value))

    ;; This is the slowest case. The attribute belongs to no specific
    ;; entity. Scan all entities in all entity maps for matches.
    :else
    (let [entity-maps (get db :workflo.entitydb.v1/data)]
      (into #{} (comp (map (fn [[entity-name entity-map]]
                             (resolve-attribute-value-in-entity-map
                              db-config entity-name entity-map attr value)))
                      cat)
            entity-maps))))


(s/fdef resolve-attr-path-into-typed-refs
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :attr-path ::specs.v1/attribute-path
               :values (s/coll-of any? :kind set?))
  :ret  (s/or :matches-found ::specs.indexes/typed-refs
              :no-matches-found #{#{}}))


(defn- resolve-attr-path-into-typed-refs
  [db db-config attr-path values]
  (reduce (fn [values attr]
            (let [result (transient #{})]
              (doseq [value (cond->> values
                              (indexes/typed-refs? (get db-config :type-map) values)
                              (map indexes/typed-ref->ref))]
                (doseq [typed-ref (resolve-attribute-value-into-typed-refs
                                   db db-config attr value)]
                  (conj! result typed-ref)))
              (persistent! result)))
          values
          (reverse attr-path)))


;;;; Public query functions


(s/fdef entities-for-attribute-path
  :args (s/cat :db ::specs.v1/entitydb
               :db-config ::specs.v1/db-config
               :path ::specs.v1/attribute-path
               :values (s/coll-of any? :kind set?))
  :ret  (s/or :matching-entities-found ::specs.v1/entities
              :no-matching-entities-found #{#{}}))


(defn ^:export entities-for-attribute-path
  "Returns the set of entities that resolves to any of the given
   `values` when following the given attribute `path`."
  [db db-config path values]
  (->> (resolve-attr-path-into-typed-refs db db-config path values)
       (into #{} (map #(get-in db `[:workflo.entitydb.v1/data ~@%])))))
