(ns pigpen-avro-parquet.core
  (:require [pigpen.raw :as raw]
            [pigpen.core :as pig]
            )

  (:import
           parquet.avro.AvroParquetReader
           org.apache.hadoop.fs.Path
           parquet.avro.AvroReadSupport
           org.apache.hadoop.conf.Configuration
           org.apache.avro.Schema))

(defmulti type->pig-type type)
(defmethod type->pig-type :default [_] "bytearray")
(defmethod type->pig-type String [_] "chararray")
(defmethod type->pig-type Float [_] "float")
(defmethod type->pig-type Double [_] "double")
(defmethod type->pig-type Integer [_] "int")
(defmethod type->pig-type Boolean [_] "boolean")

(defn avro-parquet-row->edn
  "Maps a List of NameValue records to an hashmap with the specified keys"
  [row]
  (let [result
        (->> row
             .getSchema
             .getFields
             (reduce (fn [acc field]
                       (let [k (.name field)
                             v (.get row k)]
                         (assoc acc (symbol k) v))) {}))]
    result))


(defn avro-parquet-reader
  ([location]
   (avro-parquet-reader location nil))
  ([location schema]
   (let [conf (Configuration.)
         file (Path. location)
         record-filter nil]
     (if (instance? Schema schema)
       (AvroReadSupport/setRequestedProjection conf schema))
     (AvroParquetReader. conf file record-filter))))

(defn load-avro-parquet-schema
  "Loads a parquet file, reads the first value, and returns the schema. The
  schema is a map of field name (a symbol) to the type (the pig type - a string)."
  [location]
  (->> location
       avro-parquet-reader
       (.read)
       avro-parquet-row->edn
       (map (fn [[k v]] [k (type->pig-type v)]))
       (into {})))

(defmethod pigpen.local/load "parquet.pig.ParquetLoader" [{:keys [location]}]
  (pigpen.local/load* (str "Parquet:" location)
                      (fn [on-next cancel?]
                        (with-open [reader (avro-parquet-reader location)]
                          (doseq [parquet-value (take-while (complement (some-fn nil? cancel?)) (repeatedly #(.read reader)))]
                            (let [pigpen-value (avro-parquet-row->edn parquet-value)]
                              (on-next pigpen-value)))))))

(defn augment-schema
  "If schema is not a map, get the extra schema information"
  [location schema]
  (if (map? schema)
    schema
    (let [ks (->> schema .getFields (map #(->> % .name symbol)))]
      (select-keys (load-avro-parquet-schema location) ks))))

(defn avro-load-pq
  "Schema can be either a map, where it is used literally, or a sequence, where
  it is used to select fields. Either strings, keywords, or symbols can be used
  for fields."
  ([location] (avro-load-pq location (load-avro-parquet-schema location)))
  ([location schema]
   (let [schema (augment-schema location schema)
         fields (->> schema keys (mapv (comp symbol name)))
         pig-schema (->> schema
                         (map (fn [[field type]] (str (name field) ":" type)))
                         (clojure.string/join ","))
         storage (raw/storage$
                  ;; built the jar from parquet-mr project, it's a submodule
                  ["lib/parquet-pig-bundle-1.5.0.jar"]   ;; the jar to reference
                  "parquet.pig.ParquetLoader"           ;; your loader class
                  [pig-schema])]
     (-> location
         (raw/load$ fields storage {:implicit-schema true})
         (raw/bind$ [] '(pigpen.pig/map->bind (pigpen.pig/args->map pigpen.pig/native->clojure))
                    {:args (clojure.core/mapcat (juxt str identity) fields), :field-type-in :native})))))

(defn reader [avro-parquet-file avro-schema]
  (->>
     (avro-load-pq avro-parquet-file avro-schema)
     pig/dump))
