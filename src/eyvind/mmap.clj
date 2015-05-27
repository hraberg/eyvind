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

(def ^Unsafe unsafe (let [field (doto (.getDeclaredField Unsafe "theUnsafe")
                                  (.setAccessible true))]
                      (.get field nil)))
(def ^Method mmap-c (doto (.getDeclaredMethod FileChannelImpl "map0" (into-array [Integer/TYPE Long/TYPE Long/TYPE]))
                      (.setAccessible true)))
(def ^Method unmap-c (doto (.getDeclaredMethod FileChannelImpl "unmap0" (into-array [Long/TYPE Long/TYPE]))
                       (.setAccessible true)))
(def BYTE_ARRAY_OFFSET (.arrayBaseOffset unsafe (class (byte-array 0))))

(defn round-to-4096 [x]
  (bit-and (+ x 0xfff) (bit-not 0xfff)))

(defn mmap [file size]
  (let [size (round-to-4096 (max size (.length (io/file file))))
        channel (.getChannel (doto (RandomAccessFile. (str file) "rw")
                               (.setLength size)))]
    {:file file
     :size size
     :address (.invoke mmap-c channel (object-array [(int 1) 0 size]))
     :channel channel}))

(defn fsync [{:keys [^FileChannel channel]}]
  (.force channel true))

(defn unmap [{:keys [size address ^FileChannel channel] :as mapped-file}]
  (fsync mapped-file)
  (.close channel)
  (.invoke unmap-c nil (object-array [address size])))

(defn remap [{:keys [file] :as mapped-file} new-size]
  (unmap mapped-file)
  (mmap file new-size))

(defn get-int ^long [{:keys [address]} pos]
  (.getInt unsafe (+ pos address)))

(defn put-int [{:keys [address]} pos x]
  (.putInt unsafe (+ pos address) x))

(defn get-long ^long [{:keys [address]} pos]
  (.getLong unsafe (+ pos address)))

(defn put-long [{:keys [address]} pos x]
  (.putLong unsafe (+ pos address) x))

(defn get-bytes ^bytes [{:keys [address]} pos ^bytes bytes]
  (.copyMemory unsafe nil (+ pos address) bytes BYTE_ARRAY_OFFSET (count bytes))
  bytes)

(defn put-bytes [{:keys [address]} pos ^bytes bytes]
  (.copyMemory unsafe bytes BYTE_ARRAY_OFFSET nil (+ pos address) (count bytes)))
