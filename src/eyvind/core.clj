(ns eyvind.core
  (:require [eyvind.mmap :as mmap]
            [clojure.java.io :as io])
  (:import
   [java.io RandomAccessFile]
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
   (-> (merge {:offset 0 :keydir {} :growth-factor 2 :sync? false} opts)
       (assoc :log (mmap/mmap file length)))))

(defrecord KeydirEntry [^long ts ^long value-size ^long value-offset])

(defn header ^bytes [^long ts ^long key-size ^long value-size]
  (-> (ByteBuffer/allocate 14)
      (.order (ByteOrder/nativeOrder))
      (.putLong ts)
      (.putShort (int key-size))
      (.putInt value-size)
      .array))

(defn long-bytes ^bytes [x]
  (-> (ByteBuffer/allocate 8)
      (.order (ByteOrder/nativeOrder))
      (.putLong x)
      .array))

(defn entry-crc ^long [^bytes header-bytes ^bytes key-bytes ^bytes value-bytes]
  (.getValue (doto (CRC32.)
               (.update header-bytes)
               (.update key-bytes)
               (.update value-bytes))))

(defn maybe-grow-log [{:keys [^eyvind.mmap.MappedFile log ^long offset ^long growth-factor] :as bc} ^long needed]
  (let [length (.length log)]
    (cond-> bc
      (> (+ offset needed) length) (update-in [:log] mmap/remap (* growth-factor length)))))

(defn put-entry
  ([bc ^String k ^bytes v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([bc ^long ts ^String k ^bytes v]
   (let [key-bytes (.getBytes k "UTF-8")
         tombstone? (nil? v)
         v (or v (byte-array 0))
         header-bytes (header ts (count key-bytes) (if tombstone?
                                                     -1
                                                     (count v)))
         crc-bytes (long-bytes (entry-crc header-bytes key-bytes v))
         entry-size (+ (count crc-bytes) (count header-bytes) (count key-bytes) (count v))
         {:keys [^eyvind.mmap.MappedFile log ^long offset sync?] :as bc} (maybe-grow-log bc entry-size)
         value-offset (+ offset (- entry-size (count v)))]
     (doto ^RandomAccessFile (.backing-file log)
       (.write crc-bytes)
       (.write ^bytes header-bytes)
       (.write key-bytes)
       (.write v)
       (cond-> sync? (-> .getFD .sync)))
     (-> bc
         (update-in [:offset] + entry-size)
         (update-in [:keydir] assoc k (->KeydirEntry ts (count v) value-offset))))))

(defn tombstone? [^KeydirEntry entry]
  (= -1 (.value-size entry)))

(defn get-entry [{:keys [log keydir]} k]
  (when-let [^KeydirEntry entry (get keydir k)]
    (when-not (tombstone? entry)
      (mmap/get-bytes log (.value-offset entry) (byte-array (.value-size entry))))))

(defn remove-entry [bc k]
  (-> bc
      (put-entry k nil)
      (update-in [:keydir] dissoc k)))

(defn scan-log [{:keys [^eyvind.mmap.MappedFile log keydir ^long offset] :as bc}]
  (loop [offset offset keydir keydir]
    (let [crc (mmap/get-long log offset)]
      (if (zero? crc)
        (do (.seek ^RandomAccessFile (.backing-file log) offset)
            (assoc bc :keydir keydir :offset offset))
        (let [ts (mmap/get-long log (+ 8 offset))
              key-size (mmap/get-short log (+ 16 offset))
              value-size (mmap/get-int log (+ 18 offset))
              actual-value-size (max value-size 0)
              entry-size (+ 14 key-size actual-value-size)]
          (when-not (= crc (mmap/crc-checksum log (+ 8 offset) entry-size))
            (throw (IllegalStateException. (str "CRC check failed at offset: " offset))))
          (let [key-offset (+ 22 offset)
                key-bytes (mmap/get-bytes log key-offset (byte-array key-size))
                k (String. ^bytes key-bytes "UTF-8")
                value-offset (+ key-offset key-size)
                entry (->KeydirEntry ts value-size value-offset)]
            (recur (+ value-offset actual-value-size)
                   (if (tombstone? entry)
                     (dissoc keydir k)
                     (assoc keydir k entry)))))))))

(defn hint-file ^String [{:keys [log]}]
  (str (:file log) ".hint"))

(defn write-hint-file [{:keys [keydir] :as bc}]
  (with-open [out (RandomAccessFile. (hint-file bc) "rw")]
    (doseq [[^String k ^KeydirEntry v] keydir
            :let [key-bytes (.getBytes k "UTF-8")]]
      (doto out
        (.writeLong (.ts v))
        (.writeShort (count key-bytes))
        (.writeInt (.value-size v))
        (.writeLong (.value-offset v))
        (.write key-bytes)))))

(defn read-hint-file [{:keys [log keydir] :as bc}]
  (let [hints (io/file (hint-file bc))]
    (if (.exists hints)
      (with-open [in (RandomAccessFile. hints "r")]
        (loop [offset 0 keydir keydir]
          (if (= (.getFilePointer in) (.length in))
            (assoc bc :keydir keydir :offset offset)
            (let [ts (.readLong in)
                  key-size (.readShort in)
                  value-size (.readInt in)
                  value-offset (.readLong in)
                  key-bytes (byte-array key-size)]
              (.read in key-bytes)
              (recur (max offset (+ value-offset value-size))
                     (assoc keydir (String. key-bytes "UTF-8") (->KeydirEntry ts value-size value-offset)))))))
      bc)))

(defn lru [^long size]
  (proxy [LinkedHashMap] [size 0.75 true]
    (removeEldestEntry [_]
      (> (count this) size))))

;; Consistent Hashing

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
