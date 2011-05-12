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
            [clojure-hadoop.imports :as imp])
  (:import (java.util StringTokenizer))
  (:use clojure.contrib.math clojure.test clojure-hadoop.job dgp.main))

(imp/import-io)
(imp/import-mapreduce)

(def *individuals-per-mapper* 10)
(def *tournaments* 50)

(def *operations* [
    {:action 'mutate :p 0.2}
    {:action 'crossover :p 0.3}
    {:action 'noop :p 0.5}
])

(defn dgp-map-generate-individuals
    "Mapper which generates (a large number of) individuals per mapper"
    [key value]
    (let [depth 4 width 2]
      (repeatedly
         *individuals-per-mapper*
         #(gentree-new depth width))))

(defn dgp-map-evaluate
  [offset individual]
  "Map evaluates the given individual with the training set and assigns a tournament
  number, so we basicly do tournament selection. The reducer can than decide what to
  do with a certain tournament as to evolve the population"
  [[(rand *tournaments*)
    (str [(reduce +
              (map (fn [i] (expt (-
                                   (- (+ 3 (* i i)) i)   ; Find this function :-)
                                   (evaluate-new individual {:a i :b 2 :c 8}))
                                 2))
                  (range 10)))
          (str individual)])]])

(defn dgp-reduce [key values]
  "First selects which kind of operation should be executed on this tournament. Then
  finds the best candidates from the pool and performs crossover, mutation, or whatever
  operation is selected."
  (vec (map #(vec [key (str %)])
            (let [p (rand)]
               (cond 
                 (and (< 0.15 p 0.55) (>= (count values) 2)) 
                    [['? (apply crossover-new (map #(read-string (second %)) (take 2 (sort values))))]]
                 (< p 0.15)
                    [['? (mutate-new (read-string (second (first (sort values)))) :depth 3)]]
                 :else (sort values))))))

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
;  :reduce :identity
;  :reduce-reader long-string-reduce-reader
  :input-format :text
  :output-format :text
  :compress-output false)

;(define-step evaluate-step []
;    :map dgp-map-evaluate
;    :sink :text
;    :reduce :none)
;
;(define-step order-evaluation []
;    :map :identity
;    :sink :text
;    :reduce :identity)
;
;(define-flow dgp-total []
;    (do-step evaluate-step
    ;         crossover-step
    ;         mutate-step
    ;         generate-step
;             order-evaluation))

(defjob/defjob generation-job
  :map dgp-map-generate-individuals
  :map-reader wrap/clojure-map-reader
  :map-writer wrap/clojure-writer
  :reduce :identity
  :input-format "org.apache.hadoop.examples.RandomWriter$RandomInputFormat"
  :output-format :text
  :compress-output false)
