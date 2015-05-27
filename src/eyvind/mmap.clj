(ns eyvind.mmap
  (:require [clojure.java.io :as io])
  (:import
   [java.io RandomAccessFile]
   [java.lang.reflect Field Method]
   [java.nio.channels FileChannel]
   [sun.nio.ch FileChannelImpl]
   [sun.misc Unsafe]))

;; Based on http://nyeggen.com/post/2014-05-18-memory-mapping-%3E2gb-of-data-in-java/

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

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

(defrecord MappedFile [^String file ^long length ^long address ^FileChannel channel])

(defn mmap [file ^long length]
  (let [length (round-to-4096 (max length (.length (io/file file))))
        channel (.getChannel (doto (RandomAccessFile. (str file) "rw")
                               (.setLength length)))
        address (.invoke mmap-c channel (object-array [(int 1) 0 length]))]
    (->MappedFile file length address channel)))

(defn fsync [{:keys [^FileChannel channel]}]
  (.force channel true))

(defn unmap [{:keys [length address ^FileChannel channel] :as mapped-file}]
  (fsync mapped-file)
  (.close channel)
  (.invoke unmap-c nil (object-array [address length])))

(defn remap [{:keys [file] :as mapped-file} ^long new-length]
  (unmap mapped-file)
  (mmap file new-length))

(defn get-int ^long [^MappedFile mapped-file ^long pos]
  (.getInt unsafe (+ pos (.address mapped-file))))

(defn put-int [^MappedFile mapped-file ^long pos ^long x]
  (.putInt unsafe (+ pos (.address mapped-file)) x))

(defn get-long ^long [^MappedFile mapped-file ^long pos]
  (.getLong unsafe (+ pos (.address mapped-file))))

(defn put-long [^MappedFile mapped-file ^long pos ^long x]
  (.putLong unsafe (+ pos (.address mapped-file)) x))

(defn get-bytes ^bytes [^MappedFile mapped-file ^long pos ^bytes bytes]
  (.copyMemory unsafe nil (+ pos (.address mapped-file)) bytes BYTE_ARRAY_OFFSET (count bytes))
  bytes)

(defn put-bytes [^MappedFile mapped-file ^long pos ^bytes bytes]
  (.copyMemory unsafe bytes BYTE_ARRAY_OFFSET nil (+ pos (.address mapped-file)) (count bytes)))
