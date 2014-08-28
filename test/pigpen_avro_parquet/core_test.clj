(ns pigpen-avro-parquet.core-test
  (:require [clojure.test :refer :all]
            [pigpen-avro-parquet.core :refer :all]
            [abracad (avro :as avro)])
  (:import org.apache.hadoop.fs.Path
           parquet.avro.AvroWriteSupport
           parquet.avro.AvroSchemaConverter
           org.apache.avro.generic.GenericData$Record
           parquet.hadoop.ParquetWriter))

(def schema
  (avro/parse-schema
   {:name "Record"
    :type :record
    :fields [{:name "test"
              :type :int}
             {:name "foobar"
              :type :int}]}))

(def selection-schema
  (avro/parse-schema
   {:name "Record"
    :type :record
    :fields [{:name "foobar"
              :type :int}]}))

(def records [{:test 1 :foobar 5} {:test 1 :foobar 5} {:test 1 :foobar 5}])

(defn avro-writer [file schema records]
  (try (clojure.java.io/delete-file file) (catch Exception e))
  (let [output (Path. ^String file)
        parquet-schema (.convert (AvroSchemaConverter.) schema)
        write-support (AvroWriteSupport. parquet-schema schema)
        wr (ParquetWriter. output write-support)]
    (doseq [r records]
      (let [gen (GenericData$Record. schema)]
        (doseq [k (keys r)]
          (.put gen (name k) (get r k)))
        (.write wr gen)))
    (.close wr)))

(deftest fullschema-test
  (testing "pigpen avro parquet reader fullschema-test"
    (try (clojure.java.io/delete-file "avro.parquet") (catch Exception e))
    (avro-writer "avro.parquet" schema records)
    (is (= (reader "avro.parquet" schema)
           '[{:foobar 5, :test 1} {:foobar 5, :test 1} {:foobar 5, :test 1}]))))


(deftest projection-test
  (testing "pigpen avro parquet reader projection-test"
    (try (clojure.java.io/delete-file "avro.parquet") (catch Exception e))
    (avro-writer "avro.parquet" schema records)
    (is (= (reader "avro.parquet" selection-schema)
           '[{:foobar 5} {:foobar 5} {:foobar 5}]))))

(defn -main []
  (try (clojure.java.io/delete-file "avro.parquet") (catch Exception e))
  (avro-writer "avro.parquet" schema records)
  (reader "avro.parquet" schema))
