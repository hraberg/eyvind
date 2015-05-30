(ns eyvind.core
  (:require [eyvind.mmap :as mmap]
            [clojure.java.io :as io])
  (:import
   [eyvind.mmap MappedFile]
   [java.io RandomAccessFile]
   [java.net InetAddress NetworkInterface]
   [java.nio ByteBuffer ByteOrder]
   [java.security MessageDigest]
   [java.util LinkedHashMap]
   [java.util.zip CRC32]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defrecord DiskStore [keydir sync? ^long growth-factor ^MappedFile log])
(defrecord KeydirEntry [^long ts ^long value-size ^long value-offset])

(defn open-log
  ([file]
   (open-log file {}))
  ([file {:keys [sync? growth-factor length]
          :or {sync? false growth-factor 2 length (* 8 1024)}}]
   (->DiskStore {} sync? growth-factor (mmap/mmap-file file length))))

(defn header ^bytes [^long ts ^long key-size ^long value-size]
  (-> (ByteBuffer/allocate 14)
      (.order (ByteOrder/nativeOrder))
      (.putLong ts)
      (.putShort (int key-size))
      (.putInt value-size)
      .array))

(defn long-bytes ^bytes [^long x]
  (-> (ByteBuffer/allocate 8)
      (.order (ByteOrder/nativeOrder))
      (.putLong x)
      .array))

(defn str-bytes ^bytes [^String s]
  (.getBytes s "UTF-8"))

(defn entry-crc ^long [^bytes header-bytes ^bytes key-bytes ^bytes value-bytes]
  (.getValue (doto (CRC32.)
               (.update header-bytes)
               (.update key-bytes)
               (.update value-bytes))))

(defn write-entry
  [{:keys [^long growth-factor sync?] :as bc} ^bytes header-bytes ^bytes key-bytes ^bytes v]
  (let [crc-bytes (long-bytes (entry-crc header-bytes key-bytes v))
        entry-size (+ (count crc-bytes) (count header-bytes) (count key-bytes) (count v))
        {:keys [^MappedFile log] :as bc} (update-in bc [:log] mmap/ensure-capacity growth-factor entry-size)]
    (doto ^RandomAccessFile (.backing-file log)
      (.write ^bytes crc-bytes)
      (.write header-bytes)
      (.write key-bytes)
      (.write v)
      (cond-> sync? (-> .getFD .sync)))
    bc))

(defn put-entry
  ([bc ^String k ^bytes v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([{:keys [^MappedFile log] :as bc} ^long ts ^String k ^bytes v]
   (let [key-bytes (str-bytes k)
         header-bytes (header ts (count key-bytes) (count v))
         offset (.getFilePointer ^RandomAccessFile (.backing-file log))
         value-offset (+ offset 8 (count header-bytes) (count key-bytes))]
     (-> bc
         (write-entry header-bytes key-bytes v)
         (update-in [:keydir] assoc k (->KeydirEntry ts (count v) value-offset))))))

(defn get-entry [{:keys [log keydir]} k]
  (when-let [^KeydirEntry entry (get keydir k)]
    (mmap/get-bytes log (.value-offset entry) (byte-array (.value-size entry)))))

(def tombstone-size -1)

(defn remove-entry [bc ^String k]
  (let [key-bytes (str-bytes k)
        ts (System/currentTimeMillis)]
    (write-entry bc (header ts (count key-bytes) tombstone-size) key-bytes (byte-array 0))))

(defn scan-log [{:keys [^MappedFile log keydir] :as bc}]
  (let [backing-file ^RandomAccessFile (.backing-file log)]
    (loop [offset (.getFilePointer backing-file) keydir keydir]
      (let [crc (mmap/get-long log offset)]
        (if (or (zero? crc) (= offset (.length backing-file)))
          (do (.seek backing-file offset)
              (assoc bc :keydir keydir))
          (let [ts (mmap/get-long log (+ 8 offset))
                key-size (mmap/get-short log (+ 16 offset))
                value-size (mmap/get-int log (+ 18 offset))
                tombstone? (= tombstone-size value-size)
                value-size (max value-size 0)
                entry-size (+ 14 key-size value-size)]
            (when-not (= crc (mmap/crc-checksum log (+ 8 offset) entry-size))
              (throw (IllegalStateException. (str "CRC check failed at offset: " offset))))
            (let [key-offset (+ 22 offset)
                  key-bytes (mmap/get-bytes log key-offset (byte-array key-size))
                  k (String. ^bytes key-bytes "UTF-8")
                  value-offset (+ key-offset key-size)]
              (recur (+ value-offset value-size)
                     (if tombstone?
                       (dissoc keydir k)
                       (assoc keydir k (->KeydirEntry ts value-size value-offset)))))))))))

(defn hint-file ^String [{:keys [log]}]
  (str (:file log) ".hint"))

(defn write-hint-file [{:keys [keydir] :as bc}]
  (with-open [out (RandomAccessFile. (hint-file bc) "rw")]
    (doseq [[^String k ^KeydirEntry v] keydir
            :let [key-bytes (str-bytes k)]]
      (doto out
        (.writeLong (.ts v))
        (.writeShort (count key-bytes))
        (.writeInt (.value-size v))
        (.writeLong (.value-offset v))
        (.write key-bytes)))
    bc))

(defn read-hint-file [{:keys [^MappedFile log keydir] :as bc}]
  (let [hints (io/file (hint-file bc))]
    (if (.exists hints)
      (with-open [in (RandomAccessFile. hints "r")]
        (loop [offset 0 keydir keydir]
          (if (= (.getFilePointer in) (.length in))
            (do (.seek ^RandomAccessFile (.backing-file log) offset)
                (assoc bc :keydir keydir))
            (let [ts (.readLong in)
                  key-size (.readShort in)
                  value-size (.readInt in)
                  value-offset (.readLong in)
                  key-bytes (byte-array key-size)]
              (.read in key-bytes)
              (recur (max offset (+ value-offset value-size))
                     (assoc keydir (String. key-bytes "UTF-8") (->KeydirEntry ts value-size value-offset)))))))
      bc)))

(defn init-store [log-file]
  (-> log-file open-log read-hint-file scan-log))

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

(comment

  (def bc (atom (init-store "test.log")))

  (swap! bc put-entry "foo" (.getBytes "bar" "UTF-8"))
  (String. (get-entry @bc "foo") "UTF-8"))
