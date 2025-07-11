(ns goose.brokers.jdbc.db-utils
  "Database detection and SQL generation utilities for cross-database compatibility."
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]))

(defn detect-database-type
  "Detects the database type from a connection or datasource.
  
  ### Arguments
  - `db`: Database connection or datasource
  
  ### Returns
  Keyword indicating database type: :postgresql, :sqlite, :mysql, etc."
  [db]
  (try
    (let [conn (if (instance? java.sql.Connection db)
                 db
                 (jdbc/get-connection db))
          metadata (.getMetaData conn)
          product-name (str/lower-case (.getDatabaseProductName metadata))]
      (cond
        (str/includes? product-name "postgresql") :postgresql
        (str/includes? product-name "sqlite") :sqlite
        (str/includes? product-name "mysql") :mysql
        (str/includes? product-name "h2") :h2
        :else :unknown))
    (catch Exception _
      :unknown)))

(defn quote-identifier
  "Quotes an identifier appropriately for the database type."
  [db-type identifier]
  (case db-type
    :postgresql (str "\"" identifier "\"")
    :mysql (str "`" identifier "`")
    :sqlite identifier
    identifier))

(defn limit-clause
  "Generates a LIMIT clause appropriate for the database type."
  [db-type limit]
  (case db-type
    (:postgresql :sqlite :mysql :h2) (str "LIMIT " limit)
    (str "TOP " limit)))

(defn dequeue-sql
  "Generates database-specific SQL for safely dequeuing jobs.
  
  Uses FOR UPDATE SKIP LOCKED for PostgreSQL and MySQL, transactions for others."
  [db-type enqueued-jobs-table _ready-queue]
  (case db-type
    (:postgresql :mysql)
    {:select-sql (str "SELECT id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, priority, batch_id"
                      " FROM " enqueued-jobs-table
                      " WHERE ready_queue = ?"
                      " ORDER BY priority DESC, created_at ASC"
                      " LIMIT 1 FOR UPDATE SKIP LOCKED")
     :delete-sql (str "DELETE FROM " enqueued-jobs-table " WHERE id = ?")}

    {:select-sql (str "SELECT id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, priority, batch_id"
                      " FROM " enqueued-jobs-table
                      " WHERE ready_queue = ?"
                      " ORDER BY priority DESC, created_at ASC"
                      " LIMIT 1")
     :delete-sql (str "DELETE FROM " enqueued-jobs-table " WHERE id = ?")}))

(defn batch-insert-sql
  "Generates database-specific SQL for batch inserting jobs."
  [db-type table-name columns num-rows]
  (let [quoted-table (quote-identifier db-type table-name)
        quoted-columns (str/join ", " (map #(quote-identifier db-type %) columns))
        value-placeholders (str/join ", " (repeat (count columns) "?"))
        values-clause (str/join ", " (repeat num-rows (str "(" value-placeholders ")")))]
    (str "INSERT INTO " quoted-table " (" quoted-columns ") VALUES " values-clause)))

(defn upsert-sql
  "Generates database-specific SQL for upsert operations."
  [db-type table-name columns conflict-columns update-columns]
  (let [quoted-table (quote-identifier db-type table-name)
        quoted-columns (str/join ", " (map #(quote-identifier db-type %) columns))
        value-placeholders (str/join ", " (repeat (count columns) "?"))]
    (case db-type
      :postgresql
      (let [conflict-cols (str/join ", " (map #(quote-identifier db-type %) conflict-columns))
            update-sets (str/join ", " (map #(str (quote-identifier db-type %) " = EXCLUDED." (quote-identifier db-type %)) update-columns))]
        (str "INSERT INTO " quoted-table " (" quoted-columns ") VALUES (" value-placeholders ") "
             "ON CONFLICT (" conflict-cols ") DO UPDATE SET " update-sets))

      :sqlite
      (str "INSERT OR REPLACE INTO " quoted-table " (" quoted-columns ") VALUES (" value-placeholders ")")

      :mysql
      (let [update-sets (str/join ", " (map #(str (quote-identifier db-type %) " = VALUES(" (quote-identifier db-type %) ")") update-columns))]
        (str "INSERT INTO " quoted-table " (" quoted-columns ") VALUES (" value-placeholders ") "
             "ON DUPLICATE KEY UPDATE " update-sets))

      (str "INSERT INTO " quoted-table " (" quoted-columns ") VALUES (" value-placeholders ")"))))