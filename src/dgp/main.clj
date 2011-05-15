(ns dgp.main
  (:require [clojure.zip :as zip]
            [clojure.contrib.io :as io]))

(def *operators* `[- + *])
(def *terminators* (conj (range 1 5) :a :b))

(defn count-terminals
    "Counts the number of terminals in a program"
    [program]
    (count (filter #(not(ifn? %)) (flatten program))))

(defn count-operators
    "Counts the number of operators in a program"
    [program]
    (count (filter #(ifn? %) (flatten program))))

(defn evaluate-new
    [program terminals]
    (cond
      (keyword? program) (program terminals)
      (map? program)
          (apply (eval (:fn program))
                 (map #(evaluate-new % terminals)
                      (:args program)))
      :else program))

(defn gentree-new
    ([depth width] (gentree-new depth width (vector)))
    ([depth width program]
    (if (= 0 (rand-int (* 2 depth)))
      (rand-nth *terminators*)
      {:fn (rand-nth *operators*)
       :depth depth
       :args (vec (map (fn [_] (gentree-new (dec depth) width program)) (range width)))})))

(defn program-zip
    [program]
    (zip/zipper
        #(vector? (:args %))
        #(:args %)
        (fn [node children] (assoc node :args children))
        program))
 
(defn mutate-new
    [tree & {:keys [mutation_rate max-mutations depth width]
             :or   {mutation_rate 0.15 max-mutations 2 depth 2 width 2}}]
    (let [root-depth (:depth tree)]
      (loop [zipped (program-zip tree)
             remaining max-mutations]
        (if (zip/end? zipped)
          (zip/root zipped)
          ; Decide whether to mutate the subtree
          (if (and (> remaining 0) (< (rand) mutation_rate))
              (do
                (println "Mutating node " (zip/node zipped))
                (recur (zip/next
                  (zip/replace zipped (gentree-new ((fnil max 0) (:depth (zip/node zipped)) 0) width)))
                  (dec remaining)))
              (recur (zip/next zipped) remaining))))))

(defn flatten-program
    [program]
    (loop [fp (zip/next (program-zip program))
           nodelist (vector)]
      (if (zip/end? fp)
       nodelist
       (recur (zip/next fp) (conj nodelist fp)))))

(defn random-subtree
    [program]
    (zip/node (rand-nth (flatten-program program))))

(defn crossover-new 
    "Produces a crossover between mommy and daddy
    Still needs to handle the case where depths are different"
    [mommy daddy & {:keys [mutation_rate]
                    :or   {mutation_rate 0.25}}]
    (let [root-depth (:depth mommy)]
      (loop [zipped (program-zip mommy)]
        (if (zip/end? zipped)
           (zip/root zipped)
           (if (and (not= (:depth (zip/node zipped)) root-depth) (< (rand) mutation_rate))
              (do
                (println "Crossover node " (zip/node zipped))
                (recur (zip/next
                  (zip/replace zipped (random-subtree daddy)))))
              (recur (zip/next zipped)))))))

(defn generate-population
    [size depth width]
    (vec (repeatedly size #(gentree-new depth width))))

(defn generate-population-to-file
    [outfile size depth width]
    (dotimes [n size] (spit outfile (str (gentree-new depth width) "\n") :append true)))

;(defn gentree
;    "Generates a tree with specified depth and width in prefix notation
;    e.g. (+ (- 1 2) (* 3 4))
;    
;    TODO: needs variable width per operator"
;    [tree depth width]
;    (if (zero? depth)
;        (vec (concat tree (repeatedly width #(rand-nth terminals))))
;        (vec (map (fn [_] (conj (gentree tree (dec depth) width)
;                     (rand-nth operators)))
;             (range width)))))
;
;(defn gentree-wrapper
;    "We should not need this. But the first operator from gentree is missing."
;    [tree depth width]
;    (-> (zip/seq-zip (gentree tree depth width)) zip/down zip/node))
;
;(defn mutate
;    "Mutates the given tree by replacing a random node with some small
;    probability by a newly generated tree with reasonable depth"
;    [tree & {:keys [mutation_rate max-mutations depth width]
;             :or   {mutation_rate 0.03 max-mutations 2 depth 2 width 2}}]
;    (loop [zipped (zip/seq-zip tree)
;           remaining max-mutations]
;        (if (zip/end? zipped)
;          (-> zipped zip/root)
;          ; Decide whether to mutate or not
;          (if (and (> remaining 0) (< (rand) mutation_rate))
;              (recur (zip/next
;                  (zip/replace zipped (gentree [] depth width)))
;                  (dec remaining))
;              (recur (zip/next zipped) remaining)))))
