(ns eyvind.core
  (:require [eyvind.mmap :as mmap]
            [clojure.java.io :as io])
  (:import
   [java.io DataOutputStream FileOutputStream DataInputStream FileInputStream]
   [java.net InetAddress NetworkInterface]
   [java.nio ByteBuffer ByteOrder]
   [java.security MessageDigest]
   [java.util LinkedHashMap]
   [java.util.zip CRC32]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn open-log
  ([file]
   (open-log file (* 8 1024) {}))
  ([file length opts]
   (-> (merge {:offset 0 :keydir {} :growth-factor 2} opts)
       (assoc :log (mmap/mmap file length)))))

(defrecord KeydirEntry [^long ts ^long value-size ^long value-offset])

(defn keydir-entry [ts k ^bytes v]
  (let [key-bytes (.getBytes (str k) "UTF-8")
        key-size (count key-bytes)
        value-size (count v)]
    (-> (->KeydirEntry ts value-size (+ 20 key-size))
        (assoc :bytes (-> (ByteBuffer/allocate (+ 20 key-size value-size))
                          (.order (ByteOrder/nativeOrder))
                          (.putLong ts)
                          (.putInt key-size)
                          (.putLong value-size)
                          (.put key-bytes)
                          (.put v)
                          .array)))))

(defn crc32 [^bytes bytes]
  (.getValue (doto (CRC32.)
               (.update bytes))))

(defn put-entry
  ([bc k v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([{:keys [^eyvind.mmap.MappedFile log ^long offset keydir file ^long growth-factor] :as bc} ts k v]
   (let [{:keys [bytes] :as entry} (keydir-entry ts k v)
         entry-size (+ 8 (count bytes))
         entry-start (+ 8 offset)
         keydir-entry (-> entry
                          (dissoc :bytes)
                          (update-in [:value-offset] + entry-start))
         length (.length log)
         {:keys [log] :as bc} (cond-> bc
                                (> (+ entry-size offset) length) (update-in [:log] mmap/remap (* growth-factor length)))]
     (mmap/put-long log offset (crc32 bytes))
     (mmap/put-bytes log entry-start bytes)
     (-> bc
         (update-in [:offset] + entry-size)
         (update-in [:keydir] assoc k keydir-entry)))))

(defn tombstone? [^KeydirEntry entry]
  (zero? (.value-size entry)))

(defn get-entry [{:keys [log keydir] :as bc} k]
  (when-let [^KeydirEntry entry (get keydir k)]
    (when-not (tombstone? entry)
      (mmap/get-bytes log (.value-offset entry) (byte-array (.value-size entry))))))

(defn remove-entry [bc k]
  (-> bc
      (put-entry k (byte-array 0))
      (update-in [:keydir] dissoc k)))

(defn read-entry [{:keys [log]} ^long offset]
  (let [ts (mmap/get-long log (+ 8 offset))
        key-size (mmap/get-int log (+ 16 offset))
        value-size (mmap/get-long log (+ 20 offset))
        entry-size (+ 20 key-size value-size)
        entry-bytes (mmap/get-bytes log (+ 8 offset) (byte-array entry-size))]
    (-> (->KeydirEntry ts value-size (+ offset 28 key-size))
        (assoc :key (String. ^bytes entry-bytes 20 key-size "UTF-8")
               :bytes entry-bytes))))

(defn scan-log [{:keys [log keydir ^long offset] :as bc}]
  (loop [offset offset keydir keydir]
    (let [crc (mmap/get-long log offset)]
      (if (zero? crc)
        (assoc bc :keydir keydir :offset offset)
        (let [{:keys [key bytes] :as entry} (read-entry bc offset)]
          (when-not (= crc (crc32 bytes))
            (throw (IllegalStateException. (str "CRC check failed at offset: " offset))))
          (recur (+ offset 8 (count bytes))
                 (if (tombstone? entry)
                   (dissoc keydir key)
                   (assoc keydir key (dissoc entry :bytes :key)))))))))

(defn hint-file ^String [{:keys [log]}]
  (str (:file log) ".hint"))

(defn write-hint-file [{:keys [keydir] :as bc}]
  (with-open [out (DataOutputStream. (io/output-stream (hint-file bc)))]
    (doseq [[^String k ^KeydirEntry v] keydir]
      (let [key-bytes (.getBytes k "UTF-8")
            key-size (count key-bytes)]
        (doto out
          (.writeLong (.ts v))
          (.writeInt key-size)
          (.writeLong (.value-size v))
          (.writeLong (.value-offset v))
          (.write key-bytes))))))

(defn read-hint-file [{:keys [log keydir] :as bc}]
  (let [hints (io/file (hint-file bc))]
    (if (.exists hints)
      (with-open [in (DataInputStream. (io/input-stream hints))]
        (loop [offset 0 keydir keydir]
          (if (pos? (.available in))
            (let [ts (.readLong in)
                  key-size (.readInt in)
                  value-size (.readLong in)
                  value-offset (.readLong in)
                  key-bytes (byte-array key-size)]
              (.read in key-bytes)
              (recur (max offset (+ value-offset value-size))
                     (assoc keydir (String. key-bytes "UTF-8") (->KeydirEntry ts value-size value-offset))))
            (assoc bc :keydir keydir :offset offset))))
      bc)))

(defn lru [^long size]
  (proxy [LinkedHashMap] [size 0.75 true]
    (removeEldestEntry [_]
      (> (count this) size))))

(defn sha1 [x]
  (->> (doto (MessageDigest/getInstance "SHA-1")
         (.update (-> x str (.getBytes "UTF-8"))))
       .digest
       (BigInteger. 1)))

(defn biginteger->hex [^BigInteger x]
  (format "%040x" x))

(defn hex->biginteger [x]
  (BigInteger. (str x) 16))

(defn ips []
  (->> (NetworkInterface/getNetworkInterfaces)
       enumeration-seq
       (mapcat (comp enumeration-seq #(.getInetAddresses ^NetworkInterface %)))
       (map #(.getHostAddress ^InetAddress %))))

(defn ip []
  (->> (ips)
       (remove (partial re-find #"^127\."))
       first))
