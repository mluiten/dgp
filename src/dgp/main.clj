(ns dgp.main
  (:require [clojure.zip :as zip]
            [clojure.contrib.io :as io]))

(defn log
    "Protected version of the natural log"
    [arg]
    (if (>= arg 1)
        (Math/log arg)
        1))

(def *operators* `[{:fn - :min-arity 2}
                   {:fn + :min-arity 2}
                   {:fn * :min-arity 2}
                   {:fn log :arity 1}])

(def *terminals* (concat (range 1 10) [:a]))
;(def *terminals* (concat (range 1 11) [:a :b :c :d :e :f :g :h :i]))

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
          (apply (resolve (:fn program))
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

(defn rebase-depth
    "Reconfigured the depth of a (sub)tree"
    [individual base-depth]
    (if (not (map? individual))
        individual
        (loop [zipped (program-zip individual)]
            (if (zip/end? zipped)
                (zip/root zipped)
                (if (map? (zip/node zipped))
                    (recur (zip/next (zip/replace zipped (into (zip/node zipped) {:depth (+ base-depth (count (zip/path zipped)))}))))
                    (recur (zip/next zipped)))))))

(defn mutate-new
    [tree & {:keys [mutation_rate depth width]
             :or   {mutation_rate 0.15 depth 8 width 2}}]
    (let [subtree (random-subtree tree)]
        (zip/root (zip/replace subtree
                       (gentree-new width
                                    depth
                                    "grow"
                                    (vector)
                                    (if (map? (zip/node subtree))
                                         (:depth (zip/node subtree))
                                         (if (not= nil (zip/up subtree))
                                             (+ 1 (:depth (zip/node (zip/up subtree))))
                                             0)))))))

(defn crossover-new 
    "Produces a crossover between mommy and daddy
    Still needs to handle the case where depths are different"
    [mommy daddy]
    (let [subtree-mom (random-subtree mommy)
          subtree-dad (random-subtree daddy)]
        (zip/root (zip/replace
                       subtree-mom
                      (rebase-depth (zip/node subtree-dad)
                                    (if (map? (zip/node subtree-mom))
                                        (:depth (zip/node subtree-mom))
                                        (if (not= nil (zip/up subtree-mom))
                                            (+ 1 (:depth (zip/node (zip/up subtree-mom))))
                                            0)))))))
        
(defn generate-population
    [size depth width]
    (vec (repeatedly size #(gentree-new depth width))))

(defn generate-population-to-file
    [outfile size depth width]
    (dotimes [n size] (spit outfile (str (gentree-new depth width) "\n") :append true)))
