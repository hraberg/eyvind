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

(defn log-file ^RandomAccessFile [^DiskStore bc]
  (.backing-file ^MappedFile (.log bc)))

(defn write-entry
  [^DiskStore bc ^bytes header-bytes ^bytes key-bytes ^bytes v]
  (let [crc-bytes (long-bytes (entry-crc header-bytes key-bytes v))
        entry-size (+ (count crc-bytes) (count header-bytes) (count key-bytes) (count v))
        sync? (.sync? bc)
        bc (update-in bc [:log] mmap/ensure-capacity (.growth-factor bc) entry-size)]
    (doto (log-file bc)
      (.write ^bytes crc-bytes)
      (.write header-bytes)
      (.write key-bytes)
      (.write v)
      (cond-> sync? (-> .getFD .sync)))
    bc))

(defn put-entry
  ([bc ^String k ^bytes v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([bc ^long ts ^String k ^bytes v]
   (let [key-bytes (str-bytes k)
         header-bytes (header ts (count key-bytes) (count v))
         offset (-> bc log-file .getFilePointer)
         value-offset (+ offset 8 (count header-bytes) (count key-bytes))]
     (-> bc
         (write-entry header-bytes key-bytes v)
         (update-in [:keydir] assoc k (->KeydirEntry ts (count v) value-offset))))))

(defn get-entry [^DiskStore bc k]
  (when-let [^KeydirEntry entry (get (.keydir bc) k)]
    (mmap/get-bytes (.log bc) (.value-offset entry) (byte-array (.value-size entry)))))

(def tombstone-size -1)

(defn remove-entry
  ([bc ^String k]
   (remove-entry bc (System/currentTimeMillis) k))
  ([bc ^long ts ^String k]
    (let [key-bytes (str-bytes k)
          header-bytes (header ts (count key-bytes) tombstone-size)]
      (write-entry bc header-bytes key-bytes (byte-array 0)))))

(defn scan-log [^DiskStore bc]
  (let [log (.log bc)
        backing-file (log-file bc)]
    (loop [offset (.getFilePointer backing-file) keydir (.keydir bc)]
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

(defn hint-file ^String [^DiskStore bc]
  (str (.file ^MappedFile (.log bc)) ".hint"))

(defn write-hint-file [^DiskStore bc]
  (with-open [out (RandomAccessFile. (hint-file bc) "rw")]
    (doseq [[^String k ^KeydirEntry v] (.keydir bc)
            :let [key-bytes (str-bytes k)]]
      (doto out
        (.writeLong (.ts v))
        (.writeShort (count key-bytes))
        (.writeInt (.value-size v))
        (.writeLong (.value-offset v))
        (.write key-bytes)))
    bc))

(defn read-hint-file [^DiskStore bc]
  (let [hints (io/file (hint-file bc))]
    (if (.exists hints)
      (with-open [in (RandomAccessFile. hints "r")]
        (loop [offset 0 keydir (.keydir bc)]
          (if (= (.getFilePointer in) (.length in))
            (do (-> bc log-file (.seek offset))
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

(defn init-store
  ([log-file]
   (init-store log-file {}))
  ([log-file opts]
   (-> log-file (open-log opts) read-hint-file scan-log)))

(defn lru [^long size]
  (proxy [LinkedHashMap] [size 0.75 true]
    (removeEldestEntry [_]
      (> (count this) size))))

;; Consistent Hashing

(defn message-digest ^MessageDigest []
  (MessageDigest/getInstance "SHA-1"))

(defn consistent-hash ^BigInteger [x]
  (->> (-> x str (.getBytes "UTF-8"))
       (.digest (message-digest))
       (BigInteger. 1)))

(defn consistent-long-hash ^long [x]
  (-> (ByteBuffer/wrap
       (.digest (message-digest) (-> x str (.getBytes "UTF-8"))))
      (.order (ByteOrder/nativeOrder))
      .getLong))

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

(defn node-address [ip port]
  (str "tcp://" ip ":" port))

(defn node-prefix [{:keys [ip] :as node} vnode]
  (str "node-" (:ip node) "/vnode-" vnode))

;; TODO: Figure out parititioning of keys as in the Riak explaination.
;;       This is a total stab in the dark, picks 64 partitions based on most significant bits.
;;       http://www.johnchukwuma.com/training/Riak%20Handbook.pdf
;;       I think there needs to be a mix between this and the modulo approach.

;; Central to any Riak cluster is a 160-bit integer space (often
;; referred to as "the ring") which is divided into equally-sized
;; partitions.

;; Physical servers, referred to in the cluster as "nodes", run a
;; certain number of virtual nodes, or "vnodes". Each vnode will claim
;; a partition on the ring. The number of active vnodes is determined
;; by the number of partitions into which the ring has been split, a
;; static number chosen at cluster initialisation.

(def ^:dynamic *partitions* 64)

(defn join-hash-ring [vnodes hash-ring node]
  (let [log (when (:local? node)
              {:log (init-store (str "node-" (:ip node) ".log"))})]
    (->> (for [vnode (range vnodes)
               :let [prefix (node-prefix node vnode)]]
           [(consistent-hash prefix)
            (merge {:node node :vnode vnode} log)])
         (into hash-ring))))

(defn depart-hash-ring [hash-ring node]
  (->> (range *partitions*)
       (map (comp consistent-hash (partial node-prefix node)))
       (reduce dissoc hash-ring)))

(defn create-hash-ring [servers]
  (let [partitions-per-node (long (/ ^long *partitions* (count servers)))]
    (reduce (partial join-hash-ring partitions-per-node)
            (sorted-map) servers)))

(defn nodes-for-key [hash-ring replicas k]
  (->> (concat (subseq hash-ring > (consistent-hash k))
               (cycle hash-ring))
       (take replicas)
       (map val)))

(comment

  (def bc (atom (init-store "test.log")))

  (swap! bc put-entry "foo" (.getBytes "bar" "UTF-8"))
  (String. (get-entry @bc "foo") "UTF-8")

  (let [vnodes 3
        replicas 3
        hash-ring (create-hash-ring [{:ip (ip) :port "5555" :local? true}
                                     {:ip (ip) :port "5555"}])]
    (nodes-for-key hash-ring replicas "foo")))
