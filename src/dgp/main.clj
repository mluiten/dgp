(ns dgp.main
  (:require [clojure.zip :as zip]
            [clojure.contrib.io :as io]))

(defn log
    [arg]
    (Math/log arg))

(def *operators* `[{:fn - :min-arity 2}
                   {:fn + :min-arity 2}
                   {:fn * :min-arity 2}])

(def *terminals* (conj (range 1 15) :a :b))

(defn count-terminals
    "Counts the number of terminals in a program"
    [program]
    (count (filter #(not(ifn? %)) (flatten program))))

(defn count-operators
    "Counts the number of operators in a program"
    [program]
    (count (filter #(ifn? %) (flatten program))))

(defn take-random 
    "Takes n random individuals from coll"
    [n coll]
    (if (>= n (count (distinct coll)))
      (seq coll)
      (take n (distinct (repeatedly #(rand-nth coll))))))

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
    "Generates a new tree

    Uses the max-arity is a operator does not specify a specific arity. Method can be either
    'full' or 'grow'. 'grow' allows for leaves to be selected early."
    ([max-arity max-depth method] (gentree-new max-arity max-depth method (vector) 0))
    ([max-arity max-depth method program depth]
    (if (or (>= depth max-depth)
            (and (= method "grow")
                 (> depth 0)
                 (< (rand) (/ (count *terminals*) (+ (count *terminals*) (count *operators*))))))
      (rand-nth *terminals*)
      (let [selected-operator (rand-nth *operators*)]
          {:fn (:fn selected-operator)
           :depth depth
           :args (map (fn [_] (gentree-new max-arity max-depth method program (inc depth)))
                      (if (number? (:arity selected-operator))
                        (range (:arity selected-operator))
                        (range (+ (:min-arity selected-operator) (rand-int (- max-arity 1))))))}))))

(defn program-zip
    "Zippers an individual program into a traversable tree"
    [program]
    (zip/zipper
        #(sequential? (:args %))
        #(:args %)
        (fn [node children] (assoc node :args children))
        program))

(defn flatten-program
    [program]
    (loop [fp (program-zip program)
           nodelist (vector)]
      (if (zip/end? fp)
       nodelist
       (recur (zip/next fp) (conj nodelist fp)))))
  
(defn random-subtree
    [program]
    (rand-nth (flatten-program program)))

(defn mutate-new
    [tree & {:keys [mutation_rate depth width]
             :or   {mutation_rate 0.15 depth 2 width 2}}]
    (let [subtree (random-subtree tree)]
        (zip/root (zip/replace subtree
                       (gentree-new width
                                    (- depth
                                       ((fnil min depth)
                                        (:depth (-> subtree zip/node))))
                                    (if (< (rand) 0.5) "full" "grow"))))))
   
(defn crossover-new 
    "Produces a crossover between mommy and daddy
    Still needs to handle the case where depths are different"
    [mommy daddy]
    (zip/root (zip/replace (random-subtree mommy) (zip/node (random-subtree daddy)))))
        
(defn generate-population
    [size depth width]
    (vec (repeatedly size #(gentree-new depth width))))

(defn generate-population-to-file
    [outfile size depth width]
    (dotimes [n size] (spit outfile (str (gentree-new depth width) "\n") :append true)))
