(ns eyvind.core
  (:require [clojure.java.io :as io]
            [eyvind.mmap :as mmap]
            [zeromq.zmq :as zmq])
  (:import
   [eyvind.mmap MappedFile]
   [java.io RandomAccessFile]
   [java.net InetAddress NetworkInterface]
   [java.nio ByteBuffer ByteOrder]
   [java.security MessageDigest]
   [java.util LinkedHashMap]
   [java.util.zip CRC32]))

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
      (cond-> sync? (-> .getFD .sync))) ;; TODO: explore "rwd" mode and .getChannel (.flush false)
    bc))

;; TODO: consider using ByteArray/wrap instead of strings as keys in the keydir.
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

(defn max-digest ^double []
  (Math/pow 2 (* 8 (.getDigestLength (message-digest)))))

(defn consistent-hash ^BigInteger [x]
  (->> (-> x str (.getBytes "UTF-8"))
       (.digest (message-digest))
       (BigInteger. 1)))

(defn consistent-double-hash ^double [x]
  (.doubleValue (consistent-hash x)))

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

;; http://johnleach.co.uk/downloads/slides/riak-consistent-hashing.pdf
;; http://www.johnchukwuma.com/training/Riak%20Handbook.pdf
;; http://gotocon.com/dl/goto-aar-2012/slides/SteveVinoski_BuildingDistributedSystemsWithRiakCore.pdf

;; TODO: One simple design is to have each vnode partition be its own bitcask.
;;       Every real node will have several vnodes, some "active", but potentially parts of all of them.
;;       Replication is done by sending the entire missing (by offset) log to another node missing the vnode.
;;       Potentially this could be done bittorrent-like? Though not sure about the append only then. And who sends what?
;;       Remember that due to distribution, the logs of vnodes on different nodes might not be strictly in the same order.
;;       Instead, the keydir needs to be merge aware, ie. using vector clocks.
;;       This complicates replication, as it has to be done message by message. Obsolete messages might not need to go into the log?

(def ^:dynamic *partitions* 64)
(def ^:dynamic *replicas* 3)

(defn partition-size ^double [^long partitions]
  (quot (max-digest) partitions))

(defn create-hash-ring
  ([nodes]
   (create-hash-ring nodes *partitions*))
  ([nodes ^long partitions]
   (let [[node & nodes] (sort nodes)] ;; TODO: sorting here is wrong.
     (->> nodes
          (reduce (fn [nodes node]
                    (let [n (count (set nodes))]
                      (->> (range 0 partitions (inc n))
                           (reduce (fn [nodes idx]
                                     (assoc nodes idx node)) nodes))))
                  (vec (repeat partitions node)))
          reverse
          vec))))

(defn join-hash-ring [nodes node]
  (-> nodes set (conj node)
      (create-hash-ring (count nodes))))

(defn depart-hash-ring [nodes node]
  (-> nodes set (disj node)
      (create-hash-ring (count nodes))))

(defn partition-for-key ^long [^long partitions k]
  (long (mod (inc (quot (consistent-double-hash k)
                        (partition-size partitions)))
             partitions)))

(defn nodes-for-key
  ([nodes k]
   (nodes-for-key nodes *replicas* k))
  ([nodes ^long replicas k]
   (->> (concat (drop (partition-for-key (count nodes) k) nodes)
                (cycle nodes))
        (take replicas)
        distinct)))

(defn partitions-for-node [nodes node]
  (->> nodes
       (map-indexed vector)
       (filter (comp #{node} second))
       (mapv first)))

(defn node-by-idx [hash-ring idx]
  (nth hash-ring idx))

;; G-Counter CRDT

(defn g-counter []
  {})

(defn g-counter-inc [gc k]
  (update-in gc [k] (fnil inc 0)))

(defn g-counter-merge [x y]
  (merge-with max x y))

;; Version Vectors

(defn vv [node]
  (assoc (g-counter) node 0))

(defn vv-event [vv node]
  (g-counter-inc vv node))

(defn vv-dominates? [x y]
  (boolean
   (some->> (merge-with >= x y)
            vals
            (remove number?)
            seq
            (every? true?))))

(defn vv-merge [x y]
  (g-counter-merge x y))

;; Roshi-style CRDT LWW set:

(defn lww-set []
  {:adds {} :removes {}})

(defn lww-new-timestamp? [{:keys [adds removes] :as coll} x ^long ts]
  (and (< (long (adds x 0)) ts)
       (< (long (removes x 0)) ts)))

(defn lww-set-conj
  ([coll x]
   (lww-set-conj coll x (System/currentTimeMillis)))
  ([coll x ts]
   (cond-> coll
     (lww-new-timestamp? coll x ts) (-> (update-in [:removes] dissoc x)
                                        (update-in [:adds] assoc x ts)))))

(defn lww-set-disj
  ([coll x]
   (lww-set-disj coll x (System/currentTimeMillis)))
  ([coll x ts]
   (cond-> coll
     (lww-new-timestamp? coll x ts) (-> (update-in [:adds] dissoc x)
                                        (update-in [:removes] assoc x ts)))))

(defn lww-set-contains? [{:keys [adds]} x]
  (contains? adds x))

(defn lww-set-merge [x {:keys [adds removes]}]
  (let [x (reduce (partial apply lww-set-conj) x adds)]
    (reduce (partial apply lww-set-disj) x removes)))

;; Dotted Version Vectors
;; Based on http://haslab.uminho.pt/tome/files/dvvset-dais.pdf section 6.5.

(defn dvvs [r]
  {r [0 []]})

(defn dvvs-sync [x y]
  (merge-with
   (fn [[^long n l] [^long n' l']]
     [(max n n')
      (if (> n n')
        (take (+ (- n n') (count l')) l)
        (take (+ (- n' n) (count l)) l'))])
   x y))

(defn dvvs-join [dvvs]
  (->> (for [[r [n]] dvvs]
         [r n])
       (into {})))

(defn dvvs-discard [dvvs vv]
  (->> (for [[r [^long n l]] dvvs]
         [r [n (vec (take (- n (long (vv r 0))) l))]])
       (into {})))

(defn dvvs-event [dvvs vv r v]
  (->> (for [[i [^long n l]] dvvs]
         [i (if (= i r)
              [(inc n) (vec (cons v l))]
              [(max n (long (vv i 0))) l])])
       (into {})))

(defn dvvs-values [dvvs]
  (->> (for [[_ [_ l]] dvvs]
         l)
       (apply concat)
       vec))

;; get/put interface, section 2 and 6 in dvvset-dais.pdf

(defn dvvs-get [dvvs-map k]
  (let [dvvs (dvvs-map k)]
    (with-meta (dvvs-values dvvs) {:ctx (dvvs-join dvvs)})))

(defn dvvs-put [dvvs-map r k v ctx]
  (-> dvvs-map
      (update-in [k] dvvs-discard ctx)
      (update-in [k] dvvs-event ctx r v)))

(defn dvvs-ctx [v]
  (-> v meta :ctx))

;; ZeroMQ

(defn zmq-server [context]
  (future
    (with-open [socket (-> (zmq/socket context :rep)
                           (zmq/bind "tcp://*:5555"))]
      (while (not (.isInterrupted (Thread/currentThread)))
        (println "Received " (zmq/receive-str socket))
        (zmq/send-str socket "World")))))

(defn zmq-client [context]
  (future
    (println "Connecting to hello world server...")
    (with-open [socket (-> (zmq/socket context :req)
                           (zmq/connect "tcp://127.0.0.1:5555"))]
      (dotimes [i 10]
        (let [request "Hello"]
          (println "Sending " request i "...")
          (zmq/send-str socket request)
          (println "Received " (zmq/receive-str socket) i))))))

(comment

  (def bc (atom (init-store "test.log")))

  (swap! bc put-entry "foo" (.getBytes "bar" "UTF-8"))
  (String. (get-entry @bc "foo") "UTF-8")

  (let [hash-ring (create-hash-ring (mapv (partial str (ip) "-") (range 1 6)))]
    (println (nodes-for-key hash-ring "foo"))
    (println (consistent-double-hash "foo") (partition-for-key *partitions* "foo"))
    (println (nodes-for-key (depart-hash-ring hash-ring {:ip (str (ip) "-5") :port "5555"}) "foo"))
    (println (partitions-for-node hash-ring (str (ip) "-2"))))

  (println (-> (create-hash-ring ["node1"] 8)
               (join-hash-ring "node2")
               (join-hash-ring "node3")
               (depart-hash-ring "node3")))

  (let [dvvs-map (dvvs-put {:A (dvvs :r)} :r :A :v1 {})
        get-a (dvvs-get dvvs-map :A)]
    (println get-a)
    (println (meta get-a))
    (println (dvvs-put (dvvs-put dvvs-map :r :A :v2 {})  :r :A :v3 (dvvs-ctx get-a))))

  (with-open [context (zmq/context)]
    (zmq-server context)
    @(zmq-client context)))
