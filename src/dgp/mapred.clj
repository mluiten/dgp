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
           (org.dgp RandomInputFormat)
           (org.apache.hadoop.fs FileSystem LocalFileSystem Path))
  (:use clojure.contrib.math clojure-hadoop.flow clojure-hadoop.job dgp.main))

(imp/import-io)
(imp/import-mapreduce)

(def *individuals-per-mapper* 1000)
(def *tournaments* 10)
(def *tournament-size* 5)
   
(def *num-folds* 5)

(def *operations* [
    {:action 'mutate :p 0.2}
    {:action 'crossover :p 0.3}
    {:action 'noop :p 0.5}
])

(defn dgp-map-generate-individuals
    "Mapper which generates individuals using the ramped half-and-half methodology"
    [key value]
    (let [depth 6 width 3]
      (vec (reduce concat
          (map (fn [method] (reduce concat (repeatedly
              (/ *individuals-per-mapper* depth 2)
              (fn [] (map #(vec [-1 (str (gentree-new width % method))]) (range 1 (+ 1 depth)))))))
           ["grow" "full"])))))

(def *training-set-cancer*
   [[5	1	1	1	2	1	3	1	1	2]
    [5	4	4	5	7	10	3	2	1	2]
    [3	1	1	1	2	2	3	1	1	2]
    [6	8	8	1	3	4	3	7	1	2]
    [4	1	1	3	2	1	3	1	1	2]
    [8	10	10	8	7	10	9	7	1	4]
    [1	1	1	1	2	10	3	1	1	2]
    [2	1	2	1	2	1	3	1	1	2]
    [2	1	1	1	2	1	1	1	5	2]
    [4	2	1	1	2	1	2	1	1	2]
    [1	1	1	1	1	1	3	1	1	2]
    [2	1	1	1	2	1	2	1	1	2]
    [5	3	3	3	2	3	4	4	1	4]
    [1	1	1	1	2	3	3	1	1	2]
    [8	7	5	10	7	9	5	5	4	4]
    [7	4	6	4	6	1	4	3	1	4]
    [4	1	1	1	2	1	2	1	1	2]
    [4	1	1	1	2	1	3	1	1	2]
    [10	7	7	6	4	10	4	1	2	4]
    [6	1	1	1	2	1	3	1	1	2]
    [7	3	2	10	5	10	5	4	4	4]
    [10	5	5	3	6	7	7	10	1	4]
    [3	1	1	1	2	1	2	1	1	2]
    [3	1	1	1	1	1	2	1	1	2]
    [2	1	1	1	2	1	3	1	1	2]
    [10	7	7	3	8	5	7	4	3	4]
    [2	1	1	2	2	1	3	1	1	2]
    [3	1	2	1	2	1	2	1	1	2]
    [2	1	1	1	2	1	2	1	1	2]
    [10	10	10	8	6	1	8	9	1	4]
    [6	2	1	1	1	1	7	1	1	2]
    [5	4	4	9	2	10	5	6	1	4]
    [2	5	3	3	6	7	7	5	1	4]
    [10	4	3	1	3	3	6	5	2	4]
    [6	10	10	2	8	10	7	3	3	4]
    [5	6	5	6	10	1	3	1	1	4]
    [10	10	10	4	8	1	8	10	1	4]
    [1	1	1	1	2	1	2	1	2	2]
    [3	7	7	4	4	9	4	8	1	4]
    [1	1	1	1	2	1	2	1	1	2]
    [4	1	1	3	2	1	3	1	1	2]
    [7	8	7	2	4	8	3	8	2	4]
    [9	5	8	1	2	3	2	1	5	4]
    [5	3	3	4	2	4	3	4	1	4]
    [10	3	6	2	3	5	4	10	2	4]
    [5	5	5	8	10	8	7	3	7	4]
    [10	5	5	6	8	8	7	1	1	4]
    [10	6	6	3	4	5	3	6	1	4]
    [8	10	10	1	3	6	3	9	1	4]
    [8	2	4	1	5	1	5	4	4	4]
    [5	2	3	1	6	10	5	1	1	4]
    [9	5	5	2	2	2	5	1	1	4]
    [5	3	5	5	3	3	4	10	1	4]
    [1	1	1	1	2	2	2	1	1	2]
    [9	10	10	1	10	8	3	3	1	4]
    [6	3	4	1	5	2	3	9	1	4]
    [1	1	1	1	2	1	2	1	1	2]
    [10	4	2	1	3	2	4	3	10	4]
    [4	1	1	1	2	1	3	1	1	2]
    [5	3	4	1	8	10	4	9	1	4]
    [8	3	8	3	4	9	8	9	8	4]
    [1	1	1	1	2	1	3	2	1	2]
    [5	1	3	1	2	1	2	1	1	2]
    [6	10	2	8	10	2	7	8	10	4]
    [1	3	3	2	2	1	7	2	1	2]
    [9	4	5	10	6	10	4	8	1	4]
    [10	6	4	1	3	4	3	2	3	4]
    [1	1	2	1	2	2	4	2	1	2]
    [1	1	4	1	2	1	2	1	1	2]
    [5	3	1	2	2	1	2	1	1	2]
    [3	1	1	1	2	3	3	1	1	2]
    [2	1	1	1	3	1	2	1	1	2]
    [2	2	2	1	1	1	7	1	1	2]
    [4	1	1	2	2	1	2	1	1	2]
    [5	2	1	1	2	1	3	1	1	2]
    [3	1	1	1	2	2	7	1	1	2]
    [3	5	7	8	8	9	7	10	7	4]
    [5	10	6	1	10	4	4	10	10	4]
    [3	3	6	4	5	8	4	4	1	4]
    [3	6	6	6	5	10	6	8	3	4]
    [4	1	1	1	2	1	3	1	1	2]
    [2	1	1	2	3	1	2	1	1	2]
    [1	1	1	1	2	1	3	1	1	2]
    [3	1	1	2	2	1	1	1	1	2]
    [4	1	1	1	2	1	3	1	1	2]
    [1	1	1	1	2	1	2	1	1	2]
    [2	1	1	1	2	1	3	1	1	2]
    [1	1	1	1	2	1	3	1	1	2]
    [2	1	1	2	2	1	1	1	1	2]
    [5	1	1	1	2	1	3	1	1	2]
    [9	6	9	2	10	6	2	9	10	4]
    [7	5	6	10	5	10	7	9	4	4]
    [10	3	5	1	10	5	3	10	2	4]
    [2	3	4	4	2	5	2	5	1	4]
    [4	1	2	1	2	1	3	1	1	2]
    [8	2	3	1	6	3	7	1	1	4]
    [1	3	1	2	2	2	5	3	2	2]
    [8	6	4	3	5	9	3	1	1	4]
    [10	3	3	10	2	10	7	3	3	4]
    [10	10	10	3	10	8	8	1	1	4]
    [3	3	2	1	2	3	3	1	1	2]
    [1	1	1	1	2	5	1	1	1	2]
    [8	3	3	1	2	2	3	2	1	2]
    [4	5	5	10	4	10	7	5	8	4]
    [1	1	1	1	4	3	1	1	1	2]
    [3	2	1	1	2	2	3	1	1	2]
    [1	1	2	2	2	1	3	1	1	2]
    [4	2	1	1	2	2	3	1	1	2]
    [10	10	10	2	10	10	5	3	3	4]
    [3	1	1	1	2	1	3	1	1	2]
    [8	3	5	4	5	10	1	6	2	4]
    [1	1	1	1	10	1	1	1	1	2]
    [5	1	3	1	2	1	2	1	1	2]
    [2	1	1	1	2	1	3	1	1	2]])

(def *validation-set-cancer* 
   [[5	10	8	10	8	10	3	6	3	4]
    [3	1	1	1	2	1	2	2	1	2]
    [3	1	1	1	3	1	2	1	1	2]
    [5	1	1	1	2	2	3	3	1	2]
    [4	1	1	1	2	1	2	1	1	2]
    [3	1	1	1	2	1	1	1	1	2]
    [4	1	2	1	2	1	2	1	1	2]
    [3	1	1	1	2	1	1	1	1	2]
    [2	1	1	1	2	1	1	1	1	2]
    [9	5	5	4	4	5	4	3	3	4]
    [1	1	1	1	2	5	1	1	1	2]
    [2	1	1	1	2	1	2	1	1	2]
    [3	4	5	2	6	8	4	1	1	4]
    [1	1	1	1	3	2	2	1	1	2]
    [10	10	10	10	10	1	8	8	8	4]
    [7	3	4	4	3	3	3	2	7	4]
    [10	10	10	8	2	10	4	1	1	4]
    [1	6	8	10	8	10	5	7	1	4]
    [1	1	1	1	2	1	2	3	1	2]
    [6	5	4	4	3	9	7	8	3	4]
    [1	1	1	1	2	1	3	1	1	2]
    [5	2	3	4	2	7	3	6	1	4]
    [3	2	1	1	1	1	2	1	1	2]
    [5	1	1	1	2	1	2	1	1	2]
    [2	1	1	1	2	1	2	1	1	2]
    [1	1	3	1	2	1	1	1	1	2]
    [3	1	1	3	8	1	5	8	1	2]
    [8	8	7	4	10	10	7	8	7	4]
    [1	1	1	1	1	1	3	1	1	2]
    [7	2	4	1	6	10	5	4	3	4]
    [10	10	8	6	4	5	8	10	1	4]
    [4	1	1	1	2	3	1	1	1	2]
    [5	3	5	1	8	10	5	3	1	4]
    [5	4	6	7	9	7	8	10	1	4]
    [1	1	1	1	2	1	2	1	1	2]
    [7	5	3	7	4	10	7	5	5	4]
    [1	1	1	1	2	1	1	1	1	2]
    [5	5	5	6	3	10	3	1	1	4]])

(def *validation-fold* (atom 0))

(def *training-set-math*
    (vec (map #(vec [% (- (+ (* % %) (log (+ % 3))) (+ 5 (* % % %) (+ % (* 8 %))))])
              (range 10 30))))

(def *validation-set-math*
    (vec (map #(vec [% (- (+ (* % %) (log (+ % 3))) (+ 5 (* % % %) (+ % (* 8 %))))])
              (range 30 35))))

(def *folds-math*
 (let [coll (concat *training-set-math* *validation-set-math*)]
    (partition (/ (count coll) *num-folds*) (shuffle coll))))

(defn objective-fn-math
  [individual]
  (let [training-set (apply concat (map #(if (not= @*validation-fold* %) (nth *folds-math* %)) (range *num-folds*)))]
      (* (/ (reduce +
               (map (fn [i]
                     (expt
                       (- (second i)   ; Find this function :-)
                          (evaluate-new individual {:a (first i)})) 2))
                    training-set))
         (count training-set))
      (sqrt (count (flatten-program individual))))))

(defn objective-fn-cancer
  [individual]
  (* (/ (reduce +
               (map (fn [i]
                     (expt
                       (- (last i)   ; Find this function :-)
                          (evaluate-new individual (apply hash-map (interleave [:a :b :c :d :e :f :g :h :i] (subvec i 0 9))))) 2))
                    *training-set-cancer*))
       (count *training-set-cancer*))
  (sqrt (count (flatten-program individual)))))

(defn dgp-map-evaluate
  [offset individual]
  "Map evaluates the given individual with the training set and assigns a tournament
  number, so we basicly do tournament selection. The reducer can than decide what to
  do with a certain tournament as to evolve the population"
  (let [fitness (objective-fn-math (read-string individual))] 
      [[(rand-int *tournaments*)
        (str [fitness (str individual)])]]))

(defn average
  [coll]
  (/ (apply + coll) (count coll)))

(defn- sort-and-filter
  [values]
  (sort (filter #(number? (first %)) values)))

(defn hdfs-append-outputstream
  [filename]
  (let [fs (FileSystem/get (.getConfiguration ctx/*context*))]
     (if (instance? LocalFileSystem fs)
        (ds/append-writer filename)
        (.append fs (Path. filename)))))
 
(defn dgp-reduce [key values]
  "First selects which kind of operation should be executed on this tournament. Then
  finds the best candidates from the pool and performs crossover, mutation, or whatever
  operation is selected."
  (let [sorted-values (sort-and-filter values)
        deme-size (/ (* *individuals-per-mapper* 10) *tournaments*)]
  (do 
    (ctx/with-context
      (ds/with-out-writer (hdfs-append-outputstream "best.txt")
        (println @*validation-fold* "\t"
             (.getJobName ctx/*context*) "\t"
             key "\t"
             (double (average (map first (take 10 sorted-values)))) "\t"
             (/ (reduce +
                        (map #(abs (- (second %)
                                      (evaluate-new (read-string (second (first sorted-values))) {:a (first %)})))
                                      ;(evaluate-new (read-string (second (first sorted-values))) (apply hash-map (interleave [:a :b :c :d :e :f :g :h :i] (subvec % 0 9))))) 2)
                             (nth *folds-math* @*validation-fold*)))
                (count (nth *folds-math* @*validation-fold*))) "\t"
             (second (first sorted-values)))))
    (vec (repeatedly deme-size
         (fn [] (let [p (rand)]
           (cond 
             (and (< 0.15 p 1.00) (>= (count values) 2)) 
               (do
                (ctx/increment-counter "GP Operations" "Crossover")
                (vec [-1 (str (apply crossover-new (map #(read-string (second %)) (repeatedly 2 #(first (sort (take-random *tournament-size* sorted-values)))))))]))
             (< 0.10 p 0.15)
               (do
                (ctx/increment-counter "GP Operations" "Mutation")
                (vec [-1 (str (mutate-new (read-string (second (first (sort (take-random *tournament-size* sorted-values))))) :width 3 :depth 10))]))
             :else
               (do
                (ctx/increment-counter "GP Operations" "Survival")
                (vec [-1 (second (first (sort (take-random *tournament-size* sorted-values))))]))))))))))

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

(define-source :seq [input]
  :input-format :seq
  :input input
  :map-reader wrap/clojure-map-reader)

(define-sink :seq [output]
  :output-format :seq
  :output output
  :output-key Text
  :output-value Text
  :reduce-writer wrap/clojure-writer)

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
    :sink (:seq "/tmp/generation-0")
    :replace true
    :compress-output false
    :reduce :none)

(define-step evaluate-step [name generation]
    :source (:seq (str "/tmp/generation-" generation))
    :map dgp-map-evaluate
    :sink (:seq (str "/tmp/generation-" (inc generation)))
    :replace true                   ; Should generate a new dir for each generation
    :shuffle :long-string
    :name name
    :reduce dgp-reduce
    :compress-output false)
;
;(define-step order-evaluation []
;    :map :identity
;    :sink :text
;    :reduce :identity)

(define-flow dgp-total
    [max-generations]
    (let [max-generations (Integer. max-generations)]
      (map (fn [fold] (do (do-step generate-population-step)
              (loop [generation 0]
                (if (< generation max-generations)
                (do
                  (reset! *validation-fold* fold) ; FIXME: Should be a configuration parameter of the job
                  (do-step evaluate-step (str generation) generation)
                  (recur (inc generation)))))))
      (range *num-folds*))))

(defjob/defjob generation-job
  :map dgp-map-generate-individuals
  :map-reader wrap/clojure-map-reader
  :map-writer wrap/clojure-writer
  :reduce :identity
  :input-format RandomInputFormat
  :output-format :text
  :compress-output false)
