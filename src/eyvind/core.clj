(ns eyvind.core
  (:require [clojure.java.io :as io]
            [clojure.set]
            [clojure.string :as s]
            [eyvind.mmap :as mmap]
            [zeromq.zmq :as zmq])
  (:import
   [eyvind.mmap MappedFile]
   [clojure.lang IDeref IPersistentMap IPersistentSet IPersistentVector IRecord]
   [java.io RandomAccessFile]
   [java.net InetAddress NetworkInterface]
   [java.nio ByteBuffer ByteOrder]
   [java.security MessageDigest SecureRandom]
   [java.util Base64 ConcurrentModificationException LinkedHashMap UUID]
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

(defn biginteger->hex [^BigInteger x]
  (format "%040x" x))

(defn hex->biginteger [x]
  (BigInteger. (str x) 16))

(def ^:dynamic *prgn* (SecureRandom.))

(defn random-bytes
  ([]
   (random-bytes 16))
  ([n]
   (doto (byte-array n)
     (->> (.nextBytes ^SecureRandom *prgn*)))))

(defn base64 [^bytes bs]
  (.encodeToString (Base64/getUrlEncoder) bs))

(defn ips []
  (->> (NetworkInterface/getNetworkInterfaces)
       enumeration-seq
       (remove #(.isLoopback ^NetworkInterface %))
       (mapcat (comp enumeration-seq #(.getInetAddresses ^NetworkInterface %)))
       (map #(.getHostAddress ^InetAddress %))))

(defn ip []
  (first (ips)))

(defn mac-address
  ([]
   (mac-address (ip)))
  ([ip]
   (->> ip
        InetAddress/getByName
        NetworkInterface/getByInetAddress
        .getHardwareAddress)))

(defn mac-address->hex [mac]
  (->> mac
       (map (partial format "%02X"))
       (s/join "-")))

(defn node-address [ip port]
  (str "tcp://" ip ":" port))

(def ^:dynamic *node-id* (long (biginteger (mac-address))))
(defonce node-counter (volatile! {}))

;; http://johnleach.co.uk/downloads/slides/riak-consistent-hashing.pdf
;; http://www.johnchukwuma.com/training/Riak%20Handbook.pdf
;; http://gotocon.com/dl/goto-aar-2012/slides/SteveVinoski_BuildingDistributedSystemsWithRiakCore.pdf
;; http://www.slideshare.net/eredmond/distributed-stat-structures https://github.com/coderoshi/dds

;; http://www.researchgate.net/publication/266643331_Load_Balancing_Technology_Based_On_Consistent_Hashing_For_Database_Cluster_Systems
;; In this paper nodes are never removed, but partitions are instead left empty / dead.

;; TODO: One simple design is to have each vnode partition be its own bitcask.
;;       Every real node will have several vnodes, some "active", but potentially parts of all of them.
;;       Replication is done by sending the entire missing (by offset) log to another node missing the vnode.
;;       Potentially this could be done bittorrent-like? Though not sure about the append only then. And who sends what?
;;       Remember that due to distribution, the logs of vnodes on different nodes might not be strictly in the same order.
;;       Instead, the keydir needs to be merge aware, ie. using vector clocks.
;;       This complicates replication, as it has to be done message by message. Obsolete messages might not need to go into the log?

(def ^:dynamic *partitions* 64)
(def ^:dynamic *replicas* 3)

(defn create-hash-ring
  ([nodes]
   (create-hash-ring nodes *partitions*))
  ([nodes ^long partitions]
   (->> nodes
        cycle
        (take partitions)
        vec)))

(defn join-hash-ring [nodes node]
  (let [partitions (count nodes)
        n (count (distinct nodes))]
    (->> (range n partitions (inc n))
         (reduce (fn [nodes idx]
                   (assoc nodes idx node))
                 nodes))))

(defn depart-hash-ring
  ([nodes node]
   (depart-hash-ring nodes node ::departed))
  ([nodes node reason]
   (->> nodes
        (mapv (some-fn {node reason} identity)))))

(defn partition-size ^double [^long partitions]
  (quot (max-digest) partitions))

(defn partition-for-key ^long [^long partitions k]
  (long (mod (inc (quot (double (consistent-hash k))
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

;; CRDTs
;; TODO: State deltas are just small / single element CRDTs that are joined up to the full thing:
;;       http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf
;;       http://arxiv.org/pdf/1410.2803.pdf
;;       http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-167.pdf
;;       http://www.cs.ucsb.edu/~agrawal/spring2011/ugrad/p233-wuu.pdf

;; TODO: should this be crdt-empty, crdt-merge and crdt-deref to match Clojure better?
;;       See https://github.com/funcool/cats where this would be mempty/mzero, mappend/mplus and extract/deref.

;; TODO: Lasp models dependent operations between CRDTs - like applying map to a set to maintain a dependent one:
;;       https://www.info.ucl.ac.be/~pvr/papoc-2015-lasp-abstract.pdf
;;       http://lasp-lang.org/

(defprotocol CRDT
  (crdt-least [_])
  (crdt-merge [_ other])
  (crdt-value [_]))

(prefer-method print-method IRecord IDeref)

(defn compare-> [x y]
  (pos? (compare x y)))

(extend-protocol CRDT
  IPersistentMap
  (crdt-least [this]
    (empty this))
  (crdt-merge [this other]
    (merge-with crdt-merge this other))
  (crdt-value [this]
    (into (empty this)
          (for [[k v] this]
            [k (crdt-value v)])))

  IPersistentSet
  (crdt-empty [this]
    (empty this))
  (crdt-merge [this other]
    (clojure.set/union this other))
  (crdt-value [this]
    this)

  IPersistentVector
  (crdt-least [this]
    (empty this))
  (crdt-merge [this other]
    (assert (= (count this) (count other)))
    (mapv crdt-merge this other))
  (crdt-value [this]
    (mapv crdt-value this))

  Boolean
  (crdt-least [_]
    false)
  (crdt-merge [this other]
    (boolean (or this other)))
  (crdt-value [this]
    this)

  Long
  (crdt-least [_]
    0)
  (crdt-merge [this other]
    (max (long this) (long other)))
  (crdt-value [this]
    this)

  Double
  (crdt-least [_]
    0.0)
  (crdt-merge [this other]
    (max (double this) (double other)))
  (crdt-value [this]
    this)

  Number
  (crdt-least [_]
    0N)
  (crdt-merge [this other]
    (if (pos? (compare this other))
      this
      other))
  (crdt-value [this]
    this)

  nil
  (crdt-least [_]
    nil)
  (crdt-merge [_ other]
    other)
  (crdt-value [this]
    this))

;; G-Counter CRDT
;; This is a idempotent commutative monoid, see:
;; http://noelwelsh.com/assets/downloads/scala-exchange-2013-crdt.pdf
;; In Haskell's Data.Monoid, least is called empty and merge append.
;; "A semilattice is a commutative semigroup S in which every element x is idempotent, that is, x + x = x"
;; "The algebraic preordering on S is then an ordering, given by x ≤ y if and only if x + y = y, hence all our semilattices are join-semilattices."
;; "A 0-semilattice is a semilattice which is also a monoid, or, equivalently, a semilattice which has a least element."

;; Also, Handoff-counters, allows tiers of CRDTs so clients can use them without clock explosion:
;; http://arxiv.org/pdf/1307.3207v1.pdf
;; https://github.com/pssalmeida/clj-crdt
;; The more general point around protocols / evolving processes:
;; "We have presented a solution to ECDC, called Handoff Counters, that adopts the CRDT philosophy, making the “protocol” state be a part of the CRDT state."
;; This paper makes an unrelated point about batching fsyncs and only send deltas which has been written.
;; In this gist https://gist.github.com/russelldb/f92f44bdfb619e089a4d it implies that the Riak actor (node here) is the vnode id.

(defrecord GCounter []
  CRDT
  (crdt-least [_]
    (->GCounter))
  (crdt-merge [this other]
    (merge-with crdt-merge this other))
  (crdt-value [this]
    (reduce + (vals this)))

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (compare (crdt-value this) (crdt-value other))))

(defn g-counter [node]
  (assoc (->GCounter) node 0))

(defn g-counter-inc-delta
  ([gc k]
   (g-counter-inc-delta gc k 1))
  ([gc k ^long delta]
   (assoc (crdt-least gc) k (+ delta (long (get gc k 0))))))

(defn g-counter-inc
  ([gc k]
   (g-counter-inc gc k 1))
  ([gc k delta]
   (crdt-merge gc (g-counter-inc-delta gc k delta))))

;; PN-Counter CRDT
;; This is a commutative group, aka an abelian group.
;; A group is a monoid with an inverse element, that is it can do negation, not subtraction, there's still only one merge.

(defrecord PNCounter [p n]
  CRDT
  (crdt-least [_]
    (->PNCounter (->GCounter) (->GCounter)))
  (crdt-merge [this other]
    (merge-with crdt-merge this other))
  (crdt-value [this]
    (- (long (crdt-value p)) (long (crdt-value n))))

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (compare (crdt-value this) (crdt-value other))))

(defn pn-counter [node]
  (->PNCounter (g-counter node) (g-counter node)))

(defn pn-counter-inc-delta
  ([pn k]
   (pn-counter-inc-delta pn k 1))
  ([pn k ^long delta]
   (assoc (crdt-least pn) :p (g-counter-inc-delta (:p pn) k delta))))

(defn pn-counter-inc
  ([pn k]
   (pn-counter-inc pn k 1))
  ([pn k delta]
   (crdt-merge pn (pn-counter-inc-delta pn k delta))))

(defn pn-counter-dec-delta
  ([pn k]
   (pn-counter-dec-delta pn k 1))
  ([pn k ^long delta]
   (assoc (crdt-least pn) :n (g-counter-inc-delta (:n pn) k delta))))

(defn pn-counter-dec
  ([gc k]
   (pn-counter-dec gc k 1))
  ([pn k delta]
   (crdt-merge pn (pn-counter-dec-delta pn k delta))))

;; TODO: Implement InvCRDT BoundedCounter, which uses an escrow model:
;;       http://arxiv.org/pdf/1503.09052.pdf

;; Roshi-style CRDT LWW set:
;; https://github.com/soundcloud/roshi

(declare lww-set wall-clock)

(defn lww-set-timestamp [{:keys [adds removes] :as coll} x]
  (or (adds x) (removes x)))

(defn lww-set-new-timestamp? [coll x ts]
  (compare-> ts (lww-set-timestamp coll x)))

(defn lww-set-update [from to coll x ts]
  (cond-> coll
    (lww-set-new-timestamp? coll x ts) (-> (update-in [from] dissoc x)
                                           (update-in [to] assoc x ts))))

(defrecord LWWSet [adds removes]
  CRDT
  (crdt-least [this]
    (lww-set))
  (crdt-merge [this {:keys [adds removes]}]
    (let [x (reduce (partial apply lww-set-update :removes :adds) this adds)]
      (reduce (partial apply lww-set-update :adds :removes) x removes)))
  (crdt-value [this]
    (->> adds
         keys
         (into (sorted-set-by
                (fn [x y]
                  (compare (adds x) (adds y)))))))

  IDeref
  (deref [this]
    (crdt-value this)))

(defn lww-set []
  (->LWWSet {} {}))

(defn lww-set-conj-delta [coll x ts]
  (cond-> (lww-set)
    (lww-set-new-timestamp? coll x ts) (-> (assoc-in [:adds x] ts))))

(defn lww-set-conj
  ([coll x]
   (lww-set-conj coll x (wall-clock)))
  ([coll x ts]
   (crdt-merge coll (lww-set-conj-delta coll x ts))))

(defn lww-set-disj-delta [coll x ts]
  (cond-> (lww-set)
    (lww-set-new-timestamp? coll x ts) (-> (assoc-in [:removes x] ts))))

(defn lww-set-disj
  ([coll x]
   (lww-set-disj coll x (wall-clock)))
  ([coll x ts]
   (crdt-merge coll (lww-set-disj-delta coll x ts))))

(defn lww-set-contains? [{:keys [adds]} x]
  (contains? adds x))

(defn lww-set-after [{:keys [adds removes] :as coll} ts]
  (reduce
   (fn [coll x]
     (cond-> coll
       (lww-set-new-timestamp? coll x ts) (-> (update-in [:adds] dissoc x)
                                              (update-in [:removes] dissoc x))))
   coll
   (concat (keys adds) (keys removes))))

(declare lww-map lww-reg lww-map-as-reg-map)

(defrecord LWWMap [key-set storage]
  CRDT
  (crdt-least [this]
    (lww-map))
  (crdt-merge [this other]
    (let [new-key-set (crdt-merge (.key-set this) (.key-set ^LWWMap other))]
      (->LWWMap new-key-set (into (empty storage)
                                  (select-keys (crdt-value (crdt-merge (lww-map-as-reg-map this)
                                                                       (lww-map-as-reg-map other)))
                                               (crdt-value new-key-set))))))
  (crdt-value [this]
    (crdt-value storage))

  IDeref
  (deref [this]
    (crdt-value this)))

(defn lww-map []
  (->LWWMap (lww-set) {}))

(defn lww-map-assoc-delta [{:keys [key-set] :as coll} k v ts]
  (let [delta (lww-set-conj-delta key-set k ts)]
    (cond-> (lww-map)
      (get-in delta [:adds k]) (assoc :key-set delta :storage {k v}))))

(defn lww-map-assoc
  ([coll k v]
   (lww-map-assoc coll k v (wall-clock)))
  ([coll k v ts]
   (crdt-merge coll (lww-map-assoc-delta coll k v ts))))

(defn lww-map-dissoc-delta [{:keys [key-set] :as coll} k ts]
  (let [delta (lww-set-disj-delta key-set k ts)]
    (cond-> (lww-map)
      (get-in delta [:removes k]) (assoc :key-set delta))))

(defn lww-map-dissoc
  ([coll k]
   (lww-map-dissoc coll k (wall-clock)))
  ([coll k ts]
   (crdt-merge coll (lww-map-dissoc-delta coll k ts))))

(defn lww-map-contains? [{:keys [key-set]} x]
  (lww-set-contains? key-set x))

(defn lww-map-get [{:keys [storage] :as coll} x]
  (when (lww-map-contains? coll x)
    (get storage x)))

(defn lww-map-as-reg-map [^LWWMap lww-map]
  (->> (for [[k v] (.storage lww-map)]
         [k (lww-reg (lww-set-timestamp (.key-set lww-map) k) v)])
       (into {})))

(declare or-set or-set-contains?)

(defrecord ORSet [adds removes]
  CRDT
  (crdt-least [this]
    (or-set))
  (crdt-merge [this other]
    (let [other ^ORSet other
          adds (crdt-merge adds (.adds other))
          removes (crdt-merge removes (.removes other))]
      (->ORSet (->> (select-keys removes (keys adds))
                    (merge-with clojure.set/difference adds)
                    (remove (comp empty? val))
                    (into {}))
               removes)))
  (crdt-value [this]
    (->> (keys adds)
         (filter (partial or-set-contains? this))
         set))

  IDeref
  (deref [this]
    (crdt-value this)))

(defn or-set []
  (->ORSet {} {}))

(def ^:dynamic *or-tag-fn* #(UUID/randomUUID))

(defn or-set-conj-delta [{:keys [adds]} x]
  (assoc-in (or-set) [:adds x] #{(*or-tag-fn*)}))

(defn or-set-conj [coll x]
  (crdt-merge coll (or-set-conj-delta coll x)))

(defn or-set-disj-delta [{:keys [adds]} x]
  (cond-> (or-set)
    (contains? adds x) (assoc-in [:removes x] (get adds x))))

(defn or-set-disj [coll x]
  (crdt-merge coll (or-set-disj-delta coll x)))

(defn or-set-contains? [{:keys [adds removes]} x]
  (->> (clojure.set/difference (adds x) (removes x))
       count
       pos?))

;; This is inspired by the optimized version which gc tombstones in the delta paper, http://arxiv.org/pdf/1410.2803v2.pdf
;; Doesn't necessarily work. The general idea is that the master version vector eventually will clean out the tombstone set.
;; ORSwot stands for Observe Remove Set with-out tombstones.
;; I think there's a race condition if removes are delivered out of order and target already superceded by later vv.
;; Need to understand the delta paper properly which uses dots.
;; Section 7.2 explicitly states this, a single vv only works if there's causal delivery, otherwise dots are needed.
;; Hence, we need dots. Also, it's unsure that the merge is proper, or if it has to do on both sides and then merged.
;; The delta paper also makes the distinction of delta-mutations and delta-groups, which is a join of the former.
;; These can be batched up over the network if necessary.
;; This paper should be intersting, removes the need for ordering: http://www.cmi.ac.in/~spsuresh/pdffiles/oorsets.pdf
;; Note that that paper actually advances the clock for the full set after adding the element using the old clock.

;; Attempt using DVVS.

(declare or-swot vv vv-event vv-dominates? dvvs dvvs-node dvvs-event dvvs-event-delta dvvs-discard dvvs-join)

(defrecord ORSwot [ts adds removes]
  CRDT
  (crdt-least [this]
    (or-swot))
  (crdt-merge [this other]
    (let [{:keys [ts adds removes]} (merge-with crdt-merge this other)]
      (->ORSwot ts
                (->> (for [[k v] adds
                           :when (compare-> v (removes k))]
                       [k (dvvs-discard v ts)])
                     (into {}))
                (->> (for [[k v] removes
                           :when (not (vv-dominates? ts (dvvs-join v)))]
                       [k v])
                     (into {})))))
  (crdt-value [this]
    (set (keys adds)))

  IDeref
  (deref [this]
    (crdt-value this)))

(defn or-swot
  ([]
   (or-swot *node-id*))
  ([node]
    (->ORSwot (vv node) {} {})))

(defn or-swot-conj-delta
  ([coll x]
   (or-swot-conj-delta coll x *node-id*))
  ([{:keys [ts adds]} x node]
   (let [dv (get adds x (dvvs node (get ts node)))]
     (-> (or-swot)
         (assoc :ts (vv-event ts node))
         (assoc-in [:adds x] (dvvs-event dv ts node x))))))

(defn or-swot-conj
  ([coll c]
   (or-swot-conj coll c *node-id*))
  ([coll x node]
   (crdt-merge coll (or-swot-conj-delta coll x node))))

(defn or-swot-disj-delta [{:keys [ts adds]} x]
  (cond-> (assoc (or-swot) :ts ts)
    (contains? adds x) (assoc-in [:removes x] (get adds x))))

(defn or-swot-disj [coll x]
  (crdt-merge coll (or-swot-disj-delta coll x)))

(defn or-swot-contains? [{:keys [adds]} x]
  (contains? adds x))

;; Flag CRDT

(declare flag flag-enabled?)

(defrecord Flag [storage]
  CRDT
  (crdt-least [_]
    (flag))
  (crdt-merge [this other]
    (merge-with crdt-merge this other))
  (crdt-value [this]
    (flag-enabled? this))

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (compare (crdt-value this) (crdt-value other))))

(defn flag []
  (->Flag (or-set)))

(defn flag-enable-delta [{:keys [storage]}]
  (->Flag (or-set-conj-delta storage true)))

(defn flag-enable [flag]
  (crdt-merge flag (flag-enable-delta flag)))

(defn flag-disable-delta [{:keys [storage]}]
  (->Flag (or-set-disj-delta storage true)))

(defn flag-disable [flag]
  (crdt-merge flag (flag-disable-delta flag)))

(defn flag-enabled? [{:keys [storage]}]
  (or-set-contains? storage true))


;; From http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-167.pdf
;; And https://github.com/CBaquero/delta-enabled-crdts

(defrecord LWWReg [ts value]
  CRDT
  (crdt-least [this]
    (->LWWReg (crdt-least ts) (crdt-least value)))
  (crdt-merge [this other]
    (try
      (if (compare-> other this)
        other
        this)
      (catch ConcurrentModificationException _
        (let [other ^LWWReg other]
          (->LWWReg (crdt-merge ts (.ts other))
                    (cond
                      (= value (.value other)) value
                      (satisfies? CRDT value) (crdt-merge value (.value other))
                      :else #{value (.value other)}))))))
  (crdt-value [this]
    value)

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (compare ts (.ts ^LWWReg other))))

(defn lww-reg [ts value]
  (->LWWReg ts value))

;; TODO: implement Logoot:
;;       https://hal.archives-ouvertes.fr/inria-00432368/document
;;       http://www.researchgate.net/profile/Pascal_Urso/publication/233882440_Logoot-Undo_Distributed_Collaborative_Editing_System/links/0fcfd50c84f5194937000000.pdf
;;       https://github.com/bnoguchi/logoot
;;       Alternatively, LSEQ:
;;       https://hal.archives-ouvertes.fr/hal-00921633/document
;;       https://github.com/Chat-Wane/LSEQ
;;       Also, CRDT trees:
;;       http://arxiv.org/pdf/1201.1784.pdf

;; This is a spike, no sub-ids when before and after are next to each other, and in general doesn't work very well.
;; Uses wall-clock ATM. Lot of remerging of deltas, uses Doubles instead of sub-ids. Messy merges.
;; Note, while retaining state delta, this version has tombstones which operation based Logoot shouldn't need.

(declare logoot)

(defrecord Logoot [storage]
  CRDT
  (crdt-least [this]
    (logoot))
  (crdt-merge [this other]
    (->Logoot (crdt-merge storage (.storage ^Logoot other))))
  (crdt-value [this]
    (->> (.storage ^LWWMap storage)
         (sort-by key)
         (mapv val)))

  IDeref
  (deref [this]
    (crdt-value this))

  Object
  (toString [this]
    (apply str (crdt-value this))))

(defn logoot []
  (->Logoot (->LWWMap (lww-set) (sorted-map))))

(defn logoot-between [^Logoot logoot ^double id]
  (let [ids (.storage ^LWWMap (.storage logoot))]
    [(or (some-> (rsubseq ids < id) first key) 0.0)
     (or (some-> (subseq ids >= id) first key) 1.0)]))

(defn logoot-id-at-idx [^Logoot logoot ^long idx]
  (loop [i 0 id 0.0 [[k v] & m] (seq (.storage ^LWWMap (.storage logoot)))]
    (cond (> i idx) id
          (not k) 1.0
          :else (recur (+ i (long (if (string? v) (count v) 1))) (double k) m))))

(defn logoot-id [^Logoot logoot ^long idx]
  (let [[^double before ^double after] (logoot-between logoot (logoot-id-at-idx logoot idx))]
    (+ before (double (rand (- after before))))))

(defn logoot-insert-delta [^Logoot logoot ^long idx atom]
  (let [id (logoot-id logoot idx)]
    (update-in (eyvind.core/logoot) [:storage] lww-map-assoc id atom)))

;; TODO: This should just generate a list of ids between and then add them, the paper does this.
(defn logoot-insert-deltas [^Logoot logoot ^long idx text]
  (reduce (fn [l [^long i c]]
            (crdt-merge l (logoot-insert-delta (crdt-merge l logoot) (+ i idx) (str c))))
          (eyvind.core/logoot)
          (map-indexed vector text)))

(defn logoot-insert-text [^Logoot logoot ^long idx text]
  (crdt-merge logoot (logoot-insert-deltas logoot idx text)))

(defn logoot-insert-atom [^Logoot logoot ^long idx x]
  (crdt-merge logoot (logoot-insert-delta logoot idx x)))

(defn logoot-delete-delta [^Logoot logoot ^long idx]
  (let [id (logoot-id-at-idx logoot idx)]
    (update-in (eyvind.core/logoot) [:storage] lww-map-dissoc id)))

(defn logoot-delete-deltas [^Logoot logoot ^long idx ^long length]
  (reduce (fn [l idx]
            (crdt-merge l (logoot-delete-delta (crdt-merge l logoot) idx)))
          (eyvind.core/logoot)
          (repeat length idx)))

(defn logoot-delete-text
  ([^Logoot logoot ^long idx]
   (logoot-delete-text logoot idx 1))
  ([^Logoot logoot ^long idx ^long length]
   (crdt-merge logoot (logoot-delete-deltas logoot idx length))))

(defn logoot-delete-atom [^Logoot logoot ^long idx]
  (crdt-merge logoot (logoot-delete-delta logoot idx)))

;; Logical Clocks

(defn next-node-count
  ([]
   (next-node-count node-counter *node-id*))
  ([node-counter node-id]
   (get (vswap! node-counter update-in [node-id] (fnil inc 0)) node-id)))

(defn wall-clock
  ([]
   (wall-clock *node-id*))
  ([node-id]
   [(System/currentTimeMillis) (next-node-count node-counter node-id) node-id]))

;; Version Vectors

;; From Bud:
;;   # Return true if this map is strictly smaller than or equal to the given
;;   # map. "x" is strictly smaller than or equal to "y" if:
;;   #     (a) every key in "x"  also appears in "y"
;;   #     (b) for every key k in "x", x[k] <= y[k]
(defn vv-< [x y]
  (let [y (select-keys y (keys x))]
    (and (= (count x) (count y))
         (->> (merge-with compare-> x y)
              vals
              (every? false?)))))

(defrecord VersionVector []
  CRDT
  (crdt-least [_]
    (->VersionVector))
  (crdt-merge [this other]
    (merge-with crdt-merge this other))
  (crdt-value [this]
    this)

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (cond
      (= this other) 0
      (vv-< other this) 1
      (vv-< this other) -1
      :else (throw (ConcurrentModificationException.)))))

(defn vv
  ([]
   (vv *node-id*))
  ([node]
   (assoc (->VersionVector) node 0)))

(defn vv-event-delta
  ([vv]
   (vv-event-delta vv *node-id*))
  ([vv node]
   (g-counter-inc-delta vv node)))

(defn vv-event
  ([vv]
   (vv-event vv *node-id*))
  ([vv node]
   (crdt-merge vv (vv-event-delta vv node))))

(defn vv-dominates? [x y]
  (compare-> x y))

;; Dotted Version Vectors
;; https://github.com/ricardobcl/Dotted-Version-Vectors
;; Based on http://haslab.uminho.pt/tome/files/dvvset-dais.pdf section 6.5.
;; TODO: How to represent this using primitives like LWWReg and VV?
;;       Check out this follow up paper, Global Logical Clocks:
;;       http://haslab.uminho.pt/tome/files/global_logical_clocks.pdf
;;       https://github.com/ricardobcl/GlobalLogicalClocks

(declare dvvs-join)

(defrecord DVVSet []
  CRDT
  (crdt-least [_]
    (->DVVSet))
  (crdt-merge [this other]
    (merge-with
     (fn [[^long n l] [^long n' l']]
       [(max n n')
        (vec (if (> n n')
               (take (+ (- n n') (count l')) l)
               (take (+ (- n' n) (count l)) l')))])
     this other))
  (crdt-value [this]
    (->> (for [[_ [_ l]] this]
           l)
         (apply concat)
         vec))

  IDeref
  (deref [this]
    (crdt-value this))

  Comparable
  (compareTo [this other]
    (compare (dvvs-join this) (dvvs-join other))))

(defn dvvs
  ([]
   (dvvs *node-id*))
  ([r]
   (dvvs r 0))
  ([r ts]
   (assoc (->DVVSet) r [ts []])))

(defn dvvs-sync [x y]
  (crdt-merge x y))

(defn dvvs-join [dvvs]
  (->> (for [[r [n]] dvvs]
         [r n])
       (into (->VersionVector))))

(defn dvvs-discard [dvvs vv]
  (->> (for [[r [^long n l]] dvvs]
         [r [n (vec (take (- n (long (get vv r 0))) l))]])
       (into (->DVVSet))))

(defn dvvs-event-delta
  ([dvvs vv v]
   (dvvs-event-delta dvvs vv *node-id* v))
  ([dvvs vv r v]
    (->> (for [[i [^long n l]] dvvs
               :let [ts (get vv i 0)]]
           (if (= i r)
             [i [(inc n) (vec (cons v l))]]
             (when (compare-> ts n)
               [ts l])))
         (remove nil?)
         (into (assoc (->DVVSet) r [(get vv r 0) [v]])))))

(defn dvvs-event
  ([dvvs vv v]
   (dvvs-event dvvs vv *node-id* v))
  ([dvvs vv r v]
   (crdt-merge dvvs (dvvs-event-delta dvvs vv r v))))

;; get/put interface, section 2 and 6 in dvvset-dais.pdf

(defn dvvs-get [dvvs-map k]
  (when-let [dvvs (dvvs-map k)] ;; should get dvvs values from replicas and sync into this map
    (with-meta (crdt-value dvvs) {:ctx (dvvs-join dvvs)})))

(defn dvvs-put [dvvs-map r k v ctx]
  (-> dvvs-map
      (update-in [k] dvvs-discard ctx)
      (update-in [k] dvvs-event ctx r v))) ;; should send dvvs value to replicas and sync

(defn dvvs-ctx [v]
  (-> v meta :ctx))

;; Scalable Atomic Visibility with RAMP Transactions:
;; http://www.bailis.org/papers/ramp-sigmod2014.pdf
;; https://github.com/pbailis/ramp-sigmod2014-code
;; Useful enough to implement? Can it be backed by CRDTs? Check py-impl in above repo for a Python version.
;; Paper also discusses secondary indexes a bit.
;; RAMP is a kind of 2-phase commit protocol, it doesn't necessarily benefit from CRDTs.
;; While 2-phase, it's still possible for it to fail and do partial commits across nodes / partitions.
;; RAMP-F (fast) is easiest to implement.

;; SWIM: Scalable, Weakly Consistent, Infection-Style, Membership Protocol
;; http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
;; https://speakerd.s3.amazonaws.com/presentations/5d140b302fbf01327e4e42c106afd3ef/2014-SWIM.pdf
;; https://github.com/hashicorp/memberlist
;; 200ms gossip, 1s failure
;; See also:
;; http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
;; http://czmq.zeromq.org/manual:zgossip
;; And:
;; Plumtree "Epidemic Broadcast Trees", uses a riak_dt ORSwot to share node membership, but doesn't depend on riak_core:
;; https://github.com/helium/plumtree
;; http://www.gsd.inesc-id.pt/~jleitao/pdf/srds07-leitao.pdf
;; (Doesn't seem to be a good fit? Though Riak uses it.)
;; And:
;; http://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
;; https://github.com/dominictarr/scuttlebutt
;; Also, checkout ringpop which implements both gossip and consistent hashing (without fixed partitions):
;; https://github.com/uber/ringpop

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
    (println (frequencies hash-ring))
    (println (nodes-for-key hash-ring "foo"))
    (println (consistent-hash "foo") (partition-for-key *partitions* "foo"))
    (println (nodes-for-key (depart-hash-ring hash-ring (str (ip) "-5")) "foo"))
    (println (partitions-for-node hash-ring (str (ip) "-2"))))

  (println (-> (create-hash-ring ["node1"] 8)
               (join-hash-ring "node2")
               (join-hash-ring "node3")
               (depart-hash-ring "node3")))

  (println (-> (create-hash-ring [:A :B :C] 12)
               (join-hash-ring :D)
               (depart-hash-ring :B)))

  (let [dvvs-map (dvvs-put {:A (dvvs :r)} :r :A :v1 {})
        get-a (dvvs-get dvvs-map :A)]
    (println get-a)
    (println (meta get-a))
    (println (dvvs-put (dvvs-put dvvs-map :r :A :v2 {})  :r :A :v3 (dvvs-ctx get-a))))

  (crdt-merge {:foo (lww-reg (vv :node1) #{:bar})
               :boz (lww-reg (vv :node2) #{:foo})}
              {:foo (lww-reg (vv :node1) #{:boz})
               :boz (lww-reg (vv-event (vv :node1) :node1) #{:baz})})

  (-> (lww-set)
      (lww-set-conj :a 1)
      (lww-set-conj :b 2)
      (lww-set-disj :c 3)
      (lww-set-conj :d 4)
      (lww-set-disj :d 5)
      (lww-set-disj :d 6))

  (with-open [context (zmq/context)]
    (zmq-server context)
    @(zmq-client context)))
