;; dgp.mapred -- Executing GP distributed on a Hadoop Cluster
;;
;; After compiling (see README.txt), run the example like this
;; (all on one line):
;;
;;   java -cp examples.jar clojure_hadoop.job \
;;        -job clojure-hadoop.examples.wordcount5/job \
;;        -input README.txt -output out5
;;
;; The output is plain text, written to out5/part-00000
;;
;; Notice that the strings in the output are not quoted.  In effect
;; we have come full circle to wordcount1, while maintaining the
;; separation between the mapper/reducer functions and the
;; reader/writer functions.


(ns dgp.mapred
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.context :as ctx]
            [clojure.contrib.duck-streams :as ds])
  (:import (java.util StringTokenizer)
           (org.dgp RandomInputFormat))
  (:use clojure.contrib.math clojure.test clojure-hadoop.flow clojure-hadoop.job dgp.main))

(imp/import-io)
(imp/import-mapreduce)

(def *individuals-per-mapper* 5000)
(def *tournaments* 50)
(def *tournament-size* 150)

(def *operations* [
    {:action 'mutate :p 0.2}
    {:action 'crossover :p 0.3}
    {:action 'noop :p 0.5}
])

(defn dgp-map-generate-individuals
    "Mapper which generates individuals using the ramped half-and-half methodology"
    [key value]
    (let [depth 6 width 3]
      (vec (reduce concat (repeatedly
         (/ *individuals-per-mapper* depth)
         (fn [] (map #(vec [(str (gentree-new width % (if (< (rand) 0.5) "grow" "full"))) -1]) (range 1 (+ 1 depth)))))))))

(defn objective-fn
  [individual]
  (reduce +
      (map (fn [i] (expt (-
                           (- (+ 5 (* i i) (+ i (* 8 i))) i (+ i 2))   ; Find this function :-)
                           (evaluate-new individual {:a i :b i}))
                         2))
          (range 25))))

(defn dgp-map-evaluate
  [offset individual]
  "Map evaluates the given individual with the training set and assigns a tournament
  number, so we basicly do tournament selection. The reducer can than decide what to
  do with a certain tournament as to evolve the population"
  (do 
    [[(rand-int *tournaments*)
     (str [(objective-fn (read-string individual))
          (str individual)])]]))

(defn average
  [coll]
  (quot (apply + coll) (count coll)))

(defn dgp-reduce [key values]
  "First selects which kind of operation should be executed on this tournament. Then
  finds the best candidates from the pool and performs crossover, mutation, or whatever
  operation is selected."
  (let [sorted-values (sort values)]
  (do 
    (ds/with-out-append-writer (str "best-" (.getValue (ctx/get-counter "Generations" "Current")) ".txt")
      (println "DM:" key "AVG:" (average (map first (take 25 sorted-values))) "BEST:" (first sorted-values)))
    (vec (let [p (rand)
               *deme-size* (/ (* *individuals-per-mapper* 10) *tournaments*)]
      (cond 
         (and (< 0.25 p 1.00) (>= (count values) 2)) 
           (do
            (ctx/increment-counter "GP Operations" "Crossover")
            (vec (repeatedly *deme-size*
                             (fn [] (vec [(str (apply crossover-new (map #(read-string (second %)) (take 2 (sort (take-random *tournament-size* sorted-values)))))) -1])))))
         (< p 0.25)
           (do
            (ctx/increment-counter "GP Operations" "Mutation")
            (vec (repeatedly *deme-size*
                (fn [] (vec [(str (mutate-new (read-string (second (first (sort (take-random *tournament-size* sorted-values))))) :width 3 :depth 4)) -1])))))
         :else
           (do
            (ctx/increment-counter "GP Operations" "Survival")
            sorted-values)))))))

(defn long-string-writer [^TaskInputOutputContext context key value]
  (.write context (LongWritable. key) (Text. value)))

(defn long-long-writer [^TaskInputOutputContext context key value]
  (.write context (LongWritable. key) (LongWritable. value)))

(defn long-string-reduce-reader [#^LongWritable key wvalues]
  [(.get key)
   (map (fn [#^Text v] (read-string (.toString v))) wvalues)])

(defjob/defjob evaluate-job
  :map dgp-map-evaluate
  :map-reader wrap/clojure-map-reader
  :map-writer long-string-writer
  :map-output-key LongWritable
  :reduce dgp-reduce
  :reduce-reader long-string-reduce-reader
  :reduce-writer long-string-writer
  :input-format :text
  :output-format :text
  :compress-output false)

(define-source :my-text [input]
  :input-format :text
  :input input
  :map-reader wrap/clojure-map-reader)

(define-source :random []
    :input-format RandomInputFormat
    :map-reader wrap/clojure-map-reader
    :map-writer wrap/clojure-writer)

(define-shuffle :long-string []
    :map-writer long-string-writer
    :map-output-key LongWritable
    :reduce-reader long-string-reduce-reader)

(define-step generate-population-step []
    :source :random
    :map dgp-map-generate-individuals
    :sink (:text "/tmp/generation-0")
    :replace true
    :compress-output false
    :reduce :none)

(define-step evaluate-step [generation]
    :source (:my-text (str "/tmp/generation-" generation))
    :map dgp-map-evaluate
    :sink (:text (str "/tmp/generation-" (inc generation)))
    :replace true                   ; Should generate a new dir for each generation
    :shuffle :long-string
    :reduce dgp-reduce
    :compress-output false)
;
;(define-step order-evaluation []
;    :map :identity
;    :sink :text
;    :reduce :identity)
;
(define-flow dgp-total
    [max-generations]
    (let [max-generations (Integer. max-generations)]
      (do-step generate-population-step)
        (loop [generation 0]
           (if (< generation max-generations)
               (do (do-step evaluate-step generation)
                   (ctx/increment-counter "Generations" "Current")
                   (recur (inc generation)))))))

(defjob/defjob generation-job
  :map dgp-map-generate-individuals
  :map-reader wrap/clojure-map-reader
  :map-writer wrap/clojure-writer
  :reduce :identity
  :input-format RandomInputFormat
  :output-format :text
  :compress-output false)
