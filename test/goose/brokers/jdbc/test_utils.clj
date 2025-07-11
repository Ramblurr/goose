(ns goose.brokers.jdbc.test-utils
  "Utilities for JDBC broker tests."
  (:require
   [clojure.java.shell]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :as conn]
   [next.jdbc.connection :as jdbc-conn])
  (:import
   [com.zaxxer.hikari HikariDataSource]
   [java.sql Connection]))

(def mysql-root-spec {:dbtype "mysql"
                      :dbname "mysql"
                      :host "localhost"
                      :port 3306
                      :username "root"
                      :password "root"
                      :maximumPoolSize 1})

(defn truncate-all-tables!
  "Truncates all goose tables in the database.
  This is used to clean up between test runs."
  [broker]
  (let [opts (:opts broker)
        ds (:data-source opts)]
    (conn/with-connection ds
      (fn [^java.sql.Connection db]
        (try
          (doseq [table [:enqueued-jobs :scheduled-jobs :dead-jobs :cron-jobs]]
            (try
              (cmd/purge-table! db table opts)
              (catch Exception e
                (log/warn "Failed to purge table" table e))))

          (try
            (cmd/purge-table! db :batch-jobs opts)
            (catch Exception e
              (log/warn "Failed to purge batch-jobs table" e)))

          (log/info "Truncated all JDBC broker tables")
          (catch Exception e
            (log/error "Error truncating tables" e)))))))

(defn drop-mysql-database
  [database-name]
  (with-open [root-ds (jdbc-conn/->pool HikariDataSource mysql-root-spec)]
    (conn/with-connection root-ds
      (fn [^Connection conn]
        (with-open [stmt (.createStatement conn)]
          (.execute stmt (str "DROP DATABASE IF EXISTS " database-name)))))))

(defn create-mysql-database
  [database-name]
  (with-open [root-ds (jdbc-conn/->pool HikariDataSource mysql-root-spec)]
    (conn/with-connection root-ds
      (fn [^Connection conn]
        (with-open [stmt (.createStatement conn)]
          (.execute stmt (str "CREATE DATABASE IF NOT EXISTS " database-name)))))))

(defn execute-sql-statements
  "Executes a sequence of SQL statements on the given connection."
  [^Connection conn statements]
  (doseq [sql statements]
    (when-not (str/blank? sql)
      (with-open [stmt (.createStatement conn)]
        (.execute stmt sql)))))

(defn load-and-split-sql
  "Loads SQL from file and splits into individual statements."
  [file-path]
  (-> (slurp file-path)
      (str/split #";")
      (->> (map str/trim)
           (filter #(not (str/blank? %))))))

(defn setup-schema
  [data-source ddl-file]
  (conn/with-connection data-source
    (fn [^Connection conn]
      (let [statements (load-and-split-sql ddl-file)]
        (execute-sql-statements conn statements)))))

(defn setup-sqlite-schema
  [data-source]
  (setup-schema data-source "src/goose/brokers/jdbc/schema/sqlite/tables.sql"))

(defn setup-postgresql-schema
  [data-source]
  (setup-schema data-source "src/goose/brokers/jdbc/schema/postgresql/tables.sql"))

(defn setup-mysql-schema
  [data-source database-name]
  ;; mysql doesn't support creat index if not exists, so we need to drop and recreate the database
  (drop-mysql-database database-name)
  (create-mysql-database database-name)
  (setup-schema data-source "src/goose/brokers/jdbc/schema/mysql/tables.sql"))

(defn create-sqlite-test-db
  "Creates a fresh SQLite test database with schema installed.
  Returns a map with :data-source and :db-file keys."
  []
  (let [db-file (str "/tmp/goose-test-" (System/currentTimeMillis) "-" (rand-int 10000) ".db")
        data-source (jdbc-conn/->pool HikariDataSource {:dbtype "sqlite"
                                                        :dbname db-file
                                                        :maximumPoolSize 1})]
    (setup-sqlite-schema data-source)
    {:data-source data-source :db-file db-file}))

(defn cleanup-sqlite-file
  "Cleans up SQLite temporary file."
  [db-file]
  (when db-file
    (try
      (.delete (java.io.File. db-file))
      (catch Exception _))))
