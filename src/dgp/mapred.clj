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

(def *individuals-per-mapper* 1000)
(def *tournaments* 1000)

(def *operations* [
    {:action 'mutate :p 0.2}
    {:action 'crossover :p 0.3}
    {:action 'noop :p 0.5}
])

(defn dgp-map-generate-individuals
    "Mapper which generates (a large number of) individuals per mapper"
    [key value]
    (let [depth 4 width 2]
      (vec (repeatedly
         *individuals-per-mapper*
         #(vec [(str (gentree-new width depth)) 1])))))

(defn objective-fn
  [individual]
  (reduce +
      (map (fn [i] (expt (-
                           (- (+ 5 (* i i) (+ i i)) i 8)   ; Find this function :-)
                           (evaluate-new (if (string? individual) (read-string individual) individual) {:a i :b 2 :c 8}))
                         2))
          (range 10))))

(defn dgp-map-evaluate
  [offset individual]
  "Map evaluates the given individual with the training set and assigns a tournament
  number, so we basicly do tournament selection. The reducer can than decide what to
  do with a certain tournament as to evolve the population"
  (do 
    [[(rand-int *tournaments*)
     (str [(objective-fn individual)
          (str individual)])]]))

(defn dgp-reduce [key values]
  "First selects which kind of operation should be executed on this tournament. Then
  finds the best candidates from the pool and performs crossover, mutation, or whatever
  operation is selected."
  (let [sorted-values (sort values)]
  (do 
    (ds/with-out-append-writer "best.txt"
      (println (first sorted-values)))
    (vec (let [p (rand)]
      (cond 
         (and (< 0.15 p 0.50) (>= (count values) 2)) 
          (conj sorted-values
           (do
            (ctx/increment-counter "GP Operations" "Crossover")
            [-1 (str (apply crossover-new (map #(read-string (second %)) (take 2 sorted-values))))]))
         (< p 0.15)
          (conj sorted-values
           (do
            (ctx/increment-counter "GP Operations" "Mutation")
            [-1 (str (mutate-new (read-string (second (first sorted-values))) :depth 3))]))
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
    :sink (:text "/tmp/generate_test")
    :replace true
    :compress-output false
    :reduce :none)

(define-step evaluate-step []
    :source (:my-text "/tmp/generate_test")
    :map dgp-map-evaluate
    :sink (:text "/tmp/flow_test")
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
(define-flow dgp-total []
    (do (do-step generate-population-step)
        (do-step evaluate-step)))

(defjob/defjob generation-job
  :map dgp-map-generate-individuals
  :map-reader wrap/clojure-map-reader
  :map-writer wrap/clojure-writer
  :reduce :identity
  :input-format RandomInputFormat
  :output-format :text
  :compress-output false)
