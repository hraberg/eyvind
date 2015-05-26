(ns eyvind.core
  (:import
   [java.nio ByteBuffer ByteOrder]
   [java.util.zip CRC32]))

(set! *warn-on-reflection* true)

(defn open-log
  ([file]
   (open-log file {}))
  ([file {:keys [size offset keydir] :or {size (* 1024 8) offset 0 keydir {}}}]
   {:log (eyvind.MMapper. file size) :offset 0 :size size :keydir keydir :active-file file}))

(defn keydir-entry [ts k v]
  (let [key-bytes (.getBytes (str k))
        key-size (count key-bytes)
        value-bytes (.getBytes (pr-str v))
        value-size (count value-bytes)]
    {:ts ts
     :bytes (-> (ByteBuffer/allocate (+ 24 key-size value-size))
                (.order (ByteOrder/nativeOrder))
                (.putLong ts)
                (.putLong key-size)
                (.putLong value-size)
                (.put key-bytes)
                (.put value-bytes)
                .array)
     :value-offset (+ 24 key-size)
     :value-size value-size}))

(defn crc32 [^bytes bytes]
  (.getValue (doto (CRC32.)
               (.update bytes))))

(defn put-entry
  ([bc k v]
   (put-entry bc (System/currentTimeMillis) k v))
  ([{:keys [^eyvind.MMapper log offset size keydir active-file] :as bc} ts k v]
   (let [{:keys [bytes] :as entry} (keydir-entry ts k v)
         entry-size (+ 8 (count bytes))
         entry-start (+ 8 offset)
         keydir-entry (-> entry
                          (dissoc :bytes)
                          (assoc :file active-file)
                          (update-in [:value-offset] + entry-start))
         new-size (if (> (+ entry-size offset) size)
                    (* 2 size)
                    size)]
     (when (not= new-size size)
       (.remap log new-size))
     (.putLong log offset (crc32 bytes))
     (.setBytes log entry-start bytes)
     (-> bc
         (update-in [:offset] + entry-size)
         (update-in [:keydir] assoc k keydir-entry)
         (assoc :size new-size)))))

(defn get-entry [{:keys [^eyvind.MMapper log keydir] :as bc} k]
  (when-let [{:keys [value-offset value-size]} (get keydir k)]
    (let [bytes (byte-array value-size)]
      (.getBytes log (long value-offset) bytes)
      (read-string (String. bytes "UTF-8")))))

(defn scan-log [{:keys [^eyvind.MMapper log size keydir active-file] :as bc}]
  (loop [offset 0 keydir keydir]
    (let [crc (.getLong log offset)]
      (if (zero? crc)
        (assoc bc :keydir keydir :offset offset)
        (let [ts (.getLong log (+ 8 offset))
              key-size (.getLong log (+ 16 offset))
              value-size (.getLong log (+ 24 offset))
              entry-size (+ 24 key-size value-size)
              entry-bytes (byte-array entry-size)
              value-offset (+ offset 32 key-size)]
          (.getBytes log (+ 8 offset) entry-bytes)
          (when-not (= crc (crc32 entry-bytes))
            (throw (IllegalStateException. (str "CRC check failed at offset: " offset))))
          (let [key-bytes (byte-array key-size)]
            (.getBytes log 32 key-bytes)
            (recur (+ offset 8 entry-size)
                   (assoc keydir
                          (String. key-bytes "UTF-8") {:value-offset value-offset
                                                       :value-size value-size
                                                       :ts ts
                                                       :file active-file}))))))))
