(defproject dgp "0.0.1"
   :description    "Unifying Clojure/Hadoop power!"
   :url            "http://www.artifix.net"
   :library-path   "lib/"
   :java-source-path "java/"
   :aot            [dgp.main
                    dgp.mapred]
   :dependencies   [[org.clojure/clojure "1.2.0"]
                    [org.clojure/clojure-contrib "1.2.0"]
                    [clojure-hadoop "1.3.2.3"]
                    [org.apache.hadoop/hadoop-core "0.20.2"]
                    [log4j/log4j "1.2.16"]])
