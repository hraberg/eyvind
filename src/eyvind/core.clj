(ns eyvind.core
  (:require [eyvind.mmap :as mmap])
  (:import
   [java.net InetAddress NetworkInterface]
   [java.nio ByteBuffer ByteOrder]
   [java.security MessageDigest]
   [java.util.zip CRC32]))

(set! *warn-on-reflection* true)

(defn open-log
  ([file]
   (open-log file {}))
  ([file {:keys [size offset keydir sync?] :or {size (* 1024 8) offset 0 keydir {} sync? false}}]
   {:log (mmap/mmap file size) :offset 0 :keydir keydir :file file :sync? sync?}))

(defn keydir-entry [ts k ^bytes v]
  (let [key-bytes (.getBytes (str k) "UTF-8")
        key-size (count key-bytes)
        value-size (count v)]
    {:ts ts
     :bytes (-> (ByteBuffer/allocate (+ 20 key-size value-size))
                (.order (ByteOrder/nativeOrder))
                (.putLong ts)
                (.putInt key-size)
                (.putLong value-size)
                (.put key-bytes)
                (.put v)
                .array)
     :value-size value-size
     :value-offset (+ 20 key-size)}))

(defn crc32 [^bytes bytes]
  (.getValue (doto (CRC32.)
               (.update bytes))))

(defn put-entry
  ([bc k v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([{:keys [log offset keydir file sync?] :as bc} ts k v]
   (let [{:keys [bytes] :as entry} (keydir-entry ts k v)
         entry-size (+ 8 (count bytes))
         entry-start (+ 8 offset)
         keydir-entry (-> entry
                          (dissoc :bytes)
                          (update-in [:value-offset] + entry-start))
         size (:size log)
         {:keys [log] :as bc} (cond-> bc
                                (> (+ entry-size offset) size) (update-in [:log] mmap/remap (* 2 size)))]
     (mmap/put-long log offset (crc32 bytes))
     (mmap/put-bytes log entry-start bytes)
     (when sync?
       (mmap/fsync log))
     (-> bc
         (update-in [:offset] + entry-size)
         (update-in [:keydir] assoc k keydir-entry)))))

(defn tombstone? [{:keys [value-size]}]
  (zero? value-size))

(defn get-entry [{:keys [log keydir] :as bc} k]
  (when-let [{:keys [value-offset value-size] :as entry} (get keydir k)]
    (when-not (tombstone? entry)
      (mmap/get-bytes log (long value-offset) (byte-array value-size)))))

(defn remove-entry [bc k]
  (-> bc
      (put-entry k (byte-array 0))
      (update-in [:keydir] dissoc k)))

(defn read-entry [{:keys [log]} offset]
  (let [ts (mmap/get-long log (+ 8 offset))
        key-size (mmap/get-int log (+ 16 offset))
        value-size (mmap/get-long log (+ 20 offset))
        entry-size (+ 20 key-size value-size)
        entry-bytes (mmap/get-bytes log (+ 8 offset) (byte-array entry-size))]
    {:key (String. entry-bytes 20 key-size "UTF-8")
     :ts ts
     :bytes entry-bytes
     :value-size value-size
     :value-offset (+ offset 28 key-size)}))

(defn scan-log [{:keys [log keydir] :as bc}]
  (loop [offset 0 keydir keydir]
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
