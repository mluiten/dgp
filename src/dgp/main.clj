(ns dgp.main
  (:require [clojure.zip :as zip]
            [clojure.contrib.io :as io]))

(def *operators* `[- + *])
(def *terminators* (conj (range 1 9) :a :b))

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
    ([width max-depth] (gentree-new width max-depth (vector) 0))
    ([width max-depth program depth]
    (if (or (>= depth max-depth) (= 0 (rand-int (* 2 (- max-depth depth)))))
      (rand-nth *terminators*)
      {:fn (rand-nth *operators*)
       :depth depth
       :args (vec (map (fn [_] (gentree-new width max-depth program (inc depth))) (range width)))})))

(defn program-zip
    "Zippers an individual program into a traversable tree"
    [program]
    (zip/zipper
        #(vector? (:args %))
        #(:args %)
        (fn [node children] (assoc node :args children))
        program))
 
(defn mutate-new
    [tree & {:keys [mutation_rate max-mutations depth width]
             :or   {mutation_rate 0.15 max-mutations 2 depth 2 width 2}}]
    (loop [zipped (program-zip tree)
             remaining max-mutations]
        (if (zip/end? zipped)
          (zip/root zipped)
          ; Decide whether to mutate the subtree
          (if (and (> remaining 0) (< (rand) mutation_rate))
              (recur (zip/next
                  (zip/replace zipped (gentree-new ((fnil max 0) (:depth (zip/node zipped)) 0) width)))
                  (dec remaining))
              (recur (zip/next zipped) remaining)))))

(defn flatten-program
    [program]
    (loop [fp (program-zip program)
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
    (loop [zipped (program-zip mommy)]
        (if (zip/end? zipped)
            (zip/root zipped)
            (if (< (rand) mutation_rate)
              (recur (zip/next
                  (zip/replace zipped (random-subtree daddy))))
              (recur (zip/next zipped))))))

(defn generate-population
    [size depth width]
    (vec (repeatedly size #(gentree-new depth width))))

(defn generate-population-to-file
    [outfile size depth width]
    (dotimes [n size] (spit outfile (str (gentree-new depth width) "\n") :append true)))
