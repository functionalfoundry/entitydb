(ns workflo.entitydb.util.entities
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.walk :refer [walk]]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.identity :as identity]))


;;;; Specs


(s/def ::use-spec? boolean?)
(s/def ::sample? boolean?)


(s/def ::empty-set (s/and set? empty?))
(s/def ::empty-vector (s/and vector? empty?))


;;;; Dinstinction between refs and actual entities


(s/fdef ref?
  :args (s/cat :x (s/or :entity ::specs.v1/entity
                        :ref ::specs.v1/ref
                        :map map?
                        :any any?))
  :fn   (s/or :ref
              #(and (= :ref (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-ref
              #(and (not= :ref (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn ref?
  ([x]
   (and (map? x)
        (contains? x :workflo/id)
        (= (count x) 1))))


(s/fdef refs?
  :args (s/cat :x (s/or :entities ::specs.v1/entities
                        :refs ::specs.v1/refs
                        :maps (s/coll-of map? :kind vector?)
                        :any any?))
  :fn   (s/or :refs
              #(and (= :refs (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-refs
              #(and (not= :refs (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn refs? [x]
  (and (or (set? x)
           (vector? x))
       (and (not (empty? x)))
       (every? ref? x)))


(s/fdef entity?
  :args (s/cat :x (s/or :entity ::specs.v1/entity
                        :ref ::specs.v1/ref
                        :map map?
                        :any any?)
               :opts (s/? (s/keys :opt-un [::use-spec?])))
  :fn   (s/or :entity
              #(and (= :entity (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-entity
              #(and (not= :entity (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn entity?
  ([x]
   (entity? x {:use-spec? true}))
  ([x {:keys [use-spec?]
       :or   {use-spec? true}}]
   (if use-spec?
     (s/valid? :workflo.entitydb.specs.v1/entity x)
     (and (map? x)
          (contains? x :workflo/id)
          (> (count x) 1)))))


(s/fdef entities?
  :args (s/cat :x (s/or :entities ::specs.v1/entities
                        :refs ::specs.v1/refs
                        :maps (s/coll-of map? :kind vector?)
                        :any any?)
               :opts (s/? (s/keys :opt-un [::sample? ::use-spec?])))
  :fn   (s/or :entities
              #(and (= :entities (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-entities
              #(and (not= :entities (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn entities?
  ([x]
   (entities? x {:use-spec? true}))
  ([x {:keys [sample? use-spec?]
       :or   {sample? false use-spec? true}}]
   (and (or (set? x)
            (vector? x))
        (not (empty? x))
        (every? #(entity? % {:use-spec use-spec?})
                (cond->> x
                  sample? (take 2))))))


;;;; Refifying


(s/fdef entity->ref
  :args (s/cat :entity ::specs.v1/entity)
  :ret  ::specs.v1/ref)


(defn entity->ref [entity]
  (select-keys entity [:workflo/id]))


(s/fdef refify-entity
  :args (s/cat :entity ::specs.v1/loose-entity)
  :fn   (fn [{:keys [args ret]}]
          (let [in-entity  (get args :entity)
                out-entity ret]
            (every? (fn [[k in-val]]
                      (let [out-val (get out-entity k)]
                        (cond
                          (entity? in-val) (ref? out-val)
                          (entities? in-val) (refs? out-val)
                          :else true)))
                    in-entity)))
  :ret  ::specs.v1/refified-entity)


(defn refify-entity [entity]
  (reduce (fn [out [k v]]
            (assoc out k
                   (cond
                     ;; Single reference attribute
                     (entity? v)
                     (entity->ref v)

                     ;; Multi-reference attribute
                     (entities? v)
                     (walk entity->ref identity v)

                     ;; Other attribute
                     :else v)))
          {} entity))


(s/fdef refify-entities
  :args (s/cat :entities ::specs.v1/loose-entities)
  :ret  ::specs.v1/entities)


(defn refify-entities [entities]
  (into [] (map refify-entity) entities))


;;;; Entity cloning


(defn substitute-ref-ids [entity id-map]
  (reduce (fn [out [k v]]
            (assoc out k
                   (cond
                     ;; :workflo/id attribute
                     (and (= :workflo/id k)
                          (find id-map v))
                     (get id-map v)

                     ;; Single reference attribute
                     (or (ref? v)
                         (entity? v))
                     (update v :workflo/id
                             (fn [id]
                               (or (get id-map (:workflo/id v))
                                   id)))

                     ;; Many-references attribute
                     (or (refs? v)
                         (entities? v))
                     (walk (fn [entity]
                             (update entity :workflo/id
                                     (fn [id]
                                       (or (get id-map id)
                                           id))))
                           identity v)

                     ;; Other attribute
                     :else v)))
          {} entity))


(s/fdef clone-entities
  :args (s/cat :entities ::specs.v1/entities)
  :ret  ::specs.v1/entities)


(defn clone-entities [entities]
  (let [ids    (map :workflo/id entities)
        id-map (zipmap ids (repeatedly identity/make-id))]
    (into #{} (map #(substitute-ref-ids % id-map)) entities)))


;;;; Extracting references


(s/def :workflo.entitydb.extract-references/entity ::specs.v1/refified-entity)
(s/def :workflo.entitydb.extract-references/references
  (s/or :entities ::specs.v1/loose-entities
        :empty-set ::empty-set
        :empty-vector ::empty-vector))


(s/fdef extract-references
  :args (s/cat :entity ::specs.v1/loose-entity)
  :ret  (s/keys :req-un [:workflo.entitydb.extract-references/entity
                         :workflo.entitydb.extract-references/references]))


(defn extract-references [entity]
  (let [references (transient [])
        entity*    (transient entity)]
    (doseq [[k v] entity]
      (cond
        (entity? v)
        (do
          (conj! references v)
          (assoc! entity* k (entity->ref v)))

        (entities? v)
        (do
          (doseq [other v]
            (conj! references other))
          (assoc! entity* k (into #{} (map entity->ref) v)))))
    {:entity     (-> entity*
                     (persistent!)
                     (with-meta (meta entity)))
     :references (persistent! references)}))


;;;; Recursive flattening


(s/fdef flatten-entities
  :args (s/cat :entities ::specs.v1/loose-entities)
  :ret  ::specs.v1/refified-entities)


(defn flatten-entities
  [in-entities]
  (let [out-entities (transient #{})]
    (loop [remaining in-entities]
      (if (empty? remaining)
        (persistent! out-entities)
        (let [entity (first remaining)]
          (let [{:keys [entity references]} (extract-references entity)]
            (conj! out-entities entity)
            (recur (concat (rest remaining) references))))))))


;;;; Deduplication


(s/fdef dedupe-entities
  :args (s/cat :entities ::specs.v1/entities
               :merge-fn (s/with-gen fn? #(gen/return (comp last vector))))
  :ret  ::specs.v1/entities)


(defn dedupe-entities
  [entities merge-fn]
  (into [] (map (fn [duplicates]
                  (reduce merge-fn (first duplicates) (rest duplicates))))
        (vals (group-by :workflo/id entities))))


;;;; Type deduction


(defn entity-name
  [entity type-map]
  (first (keep type-map (keys entity))))
