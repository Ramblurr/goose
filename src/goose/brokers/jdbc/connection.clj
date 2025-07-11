(ns goose.brokers.jdbc.connection
  (:import
   [java.sql Connection]
   [javax.sql DataSource]))

(defn with-connection [^DataSource ds f]
  (with-open [conn ^Connection (.getConnection ds)]
    (f conn)))
