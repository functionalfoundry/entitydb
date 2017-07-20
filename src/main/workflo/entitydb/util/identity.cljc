(ns workflo.entitydb.util.identity
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str])
  #?(:clj
     (:import [java.util UUID])))


;;;; Taken from DataScript:
;;;; https://github.com/tonsky/datascript/blob/master/src/datascript/core.cljc#L244


(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
   (defn- to-hex-string [n l]
     (let [s (.toString n 16)
           c (count s)]
       (cond
         (> c l) (subs s 0 l)
         (< c l) (str (apply str (repeat (- l c) "0")) s)
         :else   s))))


(defn- squuid
  ([]
    (squuid #?(:clj  (System/currentTimeMillis)
               :cljs (.getTime (js/Date.)))))
  ([msec]
  #?(:clj
      (let [uuid     (UUID/randomUUID)
            time     (int (/ msec 1000))
            high     (.getMostSignificantBits uuid)
            low      (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                             (bit-shift-left time 32)) ]
        (UUID. new-high low))
     :cljs
       (uuid
         (str
               (-> (int (/ msec 1000))
                   (to-hex-string 8))
           "-" (-> (rand-bits 16) (to-hex-string 4))
           "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
           "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
           "-" (-> (rand-bits 16) (to-hex-string 4))
               (-> (rand-bits 16) (to-hex-string 4))
               (-> (rand-bits 16) (to-hex-string 4)))))))


(s/fdef make-id
  :args nil?
  :ret (s/and string?
              #(= (count %) 32)))


(defn make-id
  "Returns an indexing-friendly, time-based unique entity ID."
  []
  (str/replace (str (squuid)) "-" ""))
