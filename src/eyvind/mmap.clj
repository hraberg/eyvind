(ns eyvind.mmap
  (:require [clojure.java.io :as io])
  (:import
   [java.io RandomAccessFile Closeable]
   [java.lang.reflect Field Method]
   [java.util.zip CRC32]
   [java.nio.channels FileChannel]
   [sun.nio.ch FileChannelImpl]
   [sun.misc Unsafe]))

;; Based on http://nyeggen.com/post/2014-05-18-memory-mapping-%3E2gb-of-data-in-java/

(def ^Unsafe unsafe (let [field (doto (.getDeclaredField Unsafe "theUnsafe")
                                  (.setAccessible true))]
                      (.get field nil)))
(def ^Method mmap-c (doto (.getDeclaredMethod FileChannelImpl "map0" (into-array [Integer/TYPE Long/TYPE Long/TYPE]))
                      (.setAccessible true)))
(def ^Method unmap-c (doto (.getDeclaredMethod FileChannelImpl "unmap0" (into-array [Long/TYPE Long/TYPE]))
                       (.setAccessible true)))
(def BYTE_ARRAY_OFFSET (.arrayBaseOffset unsafe (class (byte-array 0))))

(defn round-to-4096 [^long x]
  (bit-and (+ x 0xfff) (bit-not 0xfff)))

(declare unmap)

(defrecord MappedFile [^String file ^long address ^RandomAccessFile backing-file]
  Closeable
  (close [this]
    (.close backing-file)
    (unmap this)))

(defn mmap [{:keys [^RandomAccessFile backing-file] :as mapped-file} ^long length]
  (let [length (round-to-4096 (max length (.length backing-file)))
        backing-file (doto backing-file
                       (.setLength length))
        channel (.getChannel backing-file)]
    (assoc mapped-file :address (.invoke mmap-c channel (object-array [(int 1) 0 length])))))

(defn mmap-file
  ([file ^long length]
   (-> file io/file io/make-parents)
   (mmap-file file (RandomAccessFile. (io/file file) "rw") length))
  ([file ^RandomAccessFile backing-file ^long length]
   (mmap (->MappedFile file -1 backing-file) length)))

(defn unmap [{:keys [address ^RandomAccessFile backing-file] :as mapped-file}]
  (.invoke unmap-c nil (object-array [address (.length backing-file)]))
  mapped-file)

(defn remap [{:keys [backing-file] :as mapped-file} ^long new-length]
  (-> mapped-file unmap (mmap new-length)))

(defn ensure-capacity [^MappedFile log ^long growth-factor ^long needed]
  (let [backing-file ^RandomAccessFile (.backing-file log)
        length (.length backing-file)
        offset (.getFilePointer backing-file)]
    (cond-> log
      (> (+ offset needed) length) (remap (* growth-factor length)))))

(defn get-int ^long [^MappedFile mapped-file ^long pos]
  (.getInt unsafe (+ pos (.address mapped-file))))

(defn get-short ^long [^MappedFile mapped-file ^long pos]
  (long (.getShort unsafe (+ pos (.address mapped-file)))))

(defn get-long ^long [^MappedFile mapped-file ^long pos]
  (.getLong unsafe (+ pos (.address mapped-file))))

(defn get-bytes ^bytes [^MappedFile mapped-file ^long pos ^bytes bytes]
  (.copyMemory unsafe nil (+ pos (.address mapped-file)) bytes BYTE_ARRAY_OFFSET (count bytes))
  bytes)

(defn crc-checksum ^long [^MappedFile mapped-file ^long pos ^long length]
  (let [address (+ pos (.address mapped-file))
        crc (CRC32.)]
    (dotimes [n length]
      (.update crc (.getByte unsafe (+ n address))))
    (.getValue crc)))
