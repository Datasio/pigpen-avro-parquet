(defproject pigpen-avro-parquet "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.twitter/parquet-tools "1.5.0"]
                 [com.twitter/parquet-avro "1.5.0"]
                 [com.twitter/parquet-pig "1.5.0"]
                 [com.netflix.pigpen/pigpen "0.2.6"]
                 [org.apache.pig/pig "0.13.0"]
                 [org.apache.hadoop/hadoop-core "1.1.2"]
                 [org.apache.avro/avro "1.7.6"]
                 [com.damballa/abracad "0.4.9"]]
  :main ^:skip-aot pigpen-avro-parquet.core-test
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
