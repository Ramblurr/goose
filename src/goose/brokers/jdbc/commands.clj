(ns ^:no-doc goose.brokers.jdbc.commands
  "Core JDBC operations for the JDBC broker implementation."
  (:require
   [clojure.string :as str]
   [goose.brokers.jdbc.db-utils :as db-utils]
   [goose.utils :as u]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.nippy :as nippy]))

(defn serialize-job-data
  "Serializes job data (args, retry-opts, etc.) using nippy."
  [data]
  (when data
    (nippy/freeze data)))

(defn deserialize-job-data
  "Deserializes job data using nippy."
  [data]
  (when data
    (nippy/thaw data)))

(defmacro with-transaction
  "Executes body within a database transaction."
  [db & body]
  `(jdbc/with-transaction [tx# ~db]
     ~@body))

(defn table-name
  "Gets table name from options for a given table type."
  [opts table-key]
  (case table-key
    :enqueued-jobs (:enqueued-jobs-table opts)
    :scheduled-jobs (:scheduled-jobs-table opts)
    :cron-jobs (:cron-jobs-table opts)
    :dead-jobs (:dead-jobs-table opts)
    :batches (:batches-table opts)
    :batch-jobs (:batch-jobs-table opts)
    (-> table-key name (str/replace "-" "_"))))

(defn enqueue-job!
  "Enqueues a job to the ready queue table.
  
  ### Arguments
  - `db`: Database connection
  - `job`: Job map containing id, queue, ready-queue, execute-fn-sym, args, retry-opts, batch-id, etc.
  - `opts`: Options map containing table names
  
  ### Returns
  The enqueued job"
  [db job opts]
  (let [{:keys [id queue ready-queue execute-fn-sym args retry-opts enqueued-at priority batch-id]} job
        serialized-args (serialize-job-data args)
        serialized-retry-opts (serialize-job-data retry-opts)]
    (jdbc/execute! db
                   [(str "INSERT INTO " (table-name opts :enqueued-jobs) " (id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, priority, batch_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    id queue ready-queue (str execute-fn-sym) serialized-args serialized-retry-opts enqueued-at (or priority 0) batch-id])
    job))

(defn dequeue-job!
  "Dequeues the next job from a ready queue with database-specific locking.
  
  ### Arguments  
  - `db`: Database connection
  - `ready-queue`: Queue name to dequeue from
  - `opts`: Options map containing table names
  
  ### Returns
  Job map or nil if no jobs available."
  [db ready-queue opts]
  (let [db-type (db-utils/detect-database-type db)
        enqueued-jobs-table (table-name opts :enqueued-jobs)
        sql-map (db-utils/dequeue-sql db-type enqueued-jobs-table ready-queue)]
    (jdbc/with-transaction [tx db]
      (when-let [job-row (first (jdbc/execute! tx
                                               [(:select-sql sql-map) ready-queue]
                                               {:builder-fn rs/as-unqualified-lower-maps}))]
        (jdbc/execute! tx [(:delete-sql sql-map) (:id job-row)])

        {:id (:id job-row)
         :queue (:queue job-row)
         :ready-queue (:ready_queue job-row)
         :execute-fn-sym (when-let [sym-str (:execute_fn_sym job-row)]
                           (symbol sym-str))
         :args (deserialize-job-data (:args job-row))
         :retry-opts (deserialize-job-data (:retry_opts job-row))
         :enqueued-at (:enqueued_at job-row)
         :priority (:priority job-row)
         :batch-id (:batch_id job-row)}))))

(defn schedule-job!
  "Schedules a job for future execution.
  
  ### Arguments
  - `db`: Database connection  
  - `scheduled-at`: Epoch milliseconds when job should run
  - `job`: Job map
  - `opts`: Options map containing table names
  
  ### Returns
  The scheduled job"
  [db scheduled-at job opts]
  (let [{:keys [id queue ready-queue execute-fn-sym args retry-opts enqueued-at]} job
        serialized-args (serialize-job-data args)
        serialized-retry-opts (serialize-job-data retry-opts)]
    (jdbc/execute! db
                   [(str "INSERT INTO " (table-name opts :scheduled-jobs) " (id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, scheduled_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                    id queue ready-queue (str execute-fn-sym) serialized-args serialized-retry-opts enqueued-at scheduled-at])
    job))

(defn get-scheduled-jobs-due
  "Gets scheduled jobs that are ready to run (scheduled_at <= current time).
  
  ### Arguments
  - `db`: Database connection
  - `opts`: Options map containing table names
  - `current-time-ms`: Current epoch milliseconds (default: current time)
  
  ### Returns
  Sequence of job maps ready for execution."
  ([db opts] (get-scheduled-jobs-due db opts (u/epoch-time-ms)))
  ([db opts current-time-ms]
   (let [scheduled-jobs-table (table-name opts :scheduled-jobs)
         job-rows (jdbc/execute! db
                                 [(str "SELECT id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, scheduled_at"
                                       " FROM " scheduled-jobs-table
                                       " WHERE scheduled_at <= ?"
                                       " ORDER BY scheduled_at ASC")
                                  current-time-ms]
                                 {:builder-fn rs/as-unqualified-lower-maps})]
     (map (fn [job-row]
            {:id (:id job-row)
             :queue (:queue job-row)
             :ready-queue (:ready_queue job-row)
             :execute-fn-sym (when-let [sym-str (:execute_fn_sym job-row)]
                               (symbol sym-str))
             :args (deserialize-job-data (:args job-row))
             :retry-opts (deserialize-job-data (:retry_opts job-row))
             :enqueued-at (:enqueued_at job-row)
             :scheduled-at (:scheduled_at job-row)})
          job-rows))))

(defn move-scheduled-jobs-to-ready!
  "Moves scheduled jobs that are due to the ready queue.
  
  ### Arguments
  - `db`: Database connection
  - `opts`: Options map containing table names
  - `current-time-ms`: Current epoch milliseconds (default: current time)
  
  ### Returns
  Number of jobs moved."
  ([db opts] (move-scheduled-jobs-to-ready! db opts (u/epoch-time-ms)))
  ([db opts current-time-ms]
   (jdbc/with-transaction [tx db]
     (let [scheduled-jobs-table (table-name opts :scheduled-jobs)
           due-jobs (get-scheduled-jobs-due tx opts current-time-ms)]
       (doseq [job due-jobs]
         (enqueue-job! tx job opts))

       (when (seq due-jobs)
         (let [job-ids (map :id due-jobs)
               placeholders (str/join "," (repeat (count job-ids) "?"))]
           (jdbc/execute! tx (into [(str "DELETE FROM " scheduled-jobs-table " WHERE id IN (" placeholders ")")]
                                   job-ids))))

       (count due-jobs)))))

(defn move-job-to-dead!
  "Moves a failed job to the dead jobs table.
  
  ### Arguments
  - `db`: Database connection
  - `job`: Failed job map
  - `exception-info`: Map with :type, :message, :trace keys
  - `opts`: Options map containing table names
  
  ### Returns
  Updated job map"
  [db job exception-info opts]
  (let [{:keys [id queue ready-queue execute-fn-sym args retry-opts enqueued-at batch-id]} job
        {:keys [type message trace]} exception-info
        dead-jobs-table (table-name opts :dead-jobs)
        serialized-args (serialize-job-data args)
        serialized-retry-opts (serialize-job-data retry-opts)
        serialized-trace (serialize-job-data trace)
        died-at (u/epoch-time-ms)]
    (jdbc/execute! db
                   [(str "INSERT INTO " dead-jobs-table " (id, queue, ready_queue, execute_fn_sym, args, retry_opts, enqueued_at, died_at, exception_type, exception_message, exception_trace, batch_id)"
                         " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    id queue ready-queue (str execute-fn-sym) serialized-args serialized-retry-opts enqueued-at died-at (str type) message serialized-trace batch-id])
    (assoc job :died-at died-at :exception-info exception-info)))

(defn register-cron-job!
  "Registers a cron job entry.
  
  ### Arguments
  - `db`: Database connection
  - `cron-entry`: Map with :name, :cron-expression, :execute-fn-sym, :args, :queue, :ready-queue, :retry-opts
  - `opts`: Options map containing table names
  
  ### Returns
  The cron entry map"
  [db cron-entry opts]
  (let [{:keys [name cron-expression execute-fn-sym args queue ready-queue retry-opts]} cron-entry
        cron-jobs-table (table-name opts :cron-jobs)
        serialized-args (serialize-job-data args)
        serialized-retry-opts (serialize-job-data retry-opts)
        db-type (db-utils/detect-database-type db)
        columns ["name" "cron_expression" "execute_fn_sym" "args" "queue" "ready_queue" "retry_opts"]
        values [name cron-expression (str execute-fn-sym) serialized-args queue ready-queue serialized-retry-opts]
        upsert-sql (db-utils/upsert-sql db-type cron-jobs-table columns ["name"]
                                        ["cron_expression" "execute_fn_sym" "args" "queue" "ready_queue" "retry_opts"])]
    (jdbc/execute! db (into [upsert-sql] values))
    cron-entry))

(defn find-cron-job-by-name
  "Finds a cron job by its name.
  
  ### Arguments
  - `db`: Database connection
  - `cron-name`: Cron job name to search for
  - `opts`: Options map containing table names
  
  ### Returns
  Cron job map or nil if not found"
  [db cron-name opts]
  (let [cron-jobs-table (table-name opts :cron-jobs)]
    (when-let [cron-row (first (jdbc/execute! db
                                              [(str "SELECT * FROM " cron-jobs-table " WHERE name = ?") cron-name]
                                              {:builder-fn rs/as-unqualified-lower-maps}))]
      (-> cron-row
          (update :args deserialize-job-data)
          (update :retry_opts deserialize-job-data)
          (update :execute_fn_sym #(when % (symbol %)))))))

(defn delete-cron-job!
  "Deletes a cron job by its name.
  
  ### Arguments
  - `db`: Database connection
  - `cron-name`: Cron job name to delete
  - `opts`: Options map containing table names
  
  ### Returns
  Number of rows affected"
  [db cron-name opts]
  (let [cron-jobs-table (table-name opts :cron-jobs)]
    (-> (jdbc/execute! db [(str "DELETE FROM " cron-jobs-table " WHERE name = ?") cron-name])
        first
        :next.jdbc/update-count)))

(defn count-jobs
  "Counts jobs in a specific table and optionally by queue.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table keyword (:enqueued-jobs, :scheduled-jobs, :dead-jobs, etc.)
  - `opts`: Options map containing table names
  - `queue`: Optional queue name filter
  
  ### Returns
  Count of matching jobs"
  ([db table opts]
   (let [table-name-val (table-name opts table)]
     (-> (jdbc/execute! db [(str "SELECT COUNT(*) as count FROM " table-name-val)])
         first
         :count)))
  ([db table opts queue]
   (let [table-name-val (table-name opts table)]
     (-> (jdbc/execute! db [(str "SELECT COUNT(*) as count FROM " table-name-val " WHERE queue = ?") queue])
         first
         :count))))

(defn find-job-by-id
  "Finds a job by its ID in a specific table.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table keyword (:enqueued-jobs, :scheduled-jobs, :dead-jobs, etc.)
  - `job-id`: Job ID to search for
  - `opts`: Options map containing table names
  
  ### Returns
  Job map or nil if not found"
  [db table job-id opts]
  (let [table-name-val (table-name opts table)]
    (when-let [job-row (first (jdbc/execute! db
                                             [(str "SELECT * FROM " table-name-val " WHERE id = ?") job-id]
                                             {:builder-fn rs/as-unqualified-lower-maps}))]
      (-> job-row
          (update :args deserialize-job-data)
          (update :retry_opts deserialize-job-data)
          (update :execute_fn_sym #(when % (symbol %)))))))

(defn delete-job-by-id!
  "Deletes a job by its ID from a specific table.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table keyword (:enqueued-jobs, :scheduled-jobs, :dead-jobs, etc.)
  - `job-id`: Job ID to delete
  - `opts`: Options map containing table names
  
  ### Returns
  Number of rows affected"
  [db table job-id opts]
  (let [table-name-val (table-name opts table)]
    (-> (jdbc/execute! db [(str "DELETE FROM " table-name-val " WHERE id = ?") job-id])
        first
        :next.jdbc/update-count)))

(defn purge-table!
  "Deletes all jobs from a specific table, optionally filtered by queue.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table keyword (:enqueued-jobs, :scheduled-jobs, :dead-jobs, etc.)
  - `opts`: Options map containing table names
  - `queue`: Optional queue name filter
  
  ### Returns
  Number of rows deleted"
  ([db table opts]
   (let [table-name-val (table-name opts table)]
     (-> (jdbc/execute! db [(str "DELETE FROM " table-name-val)])
         first
         :next.jdbc/update-count)))
  ([db table opts queue]
   (let [table-name-val (table-name opts table)]
     (-> (jdbc/execute! db [(str "DELETE FROM " table-name-val " WHERE queue = ?") queue])
         first
         :next.jdbc/update-count))))

(defn enqueue-batch!
  "Enqueues a batch and its associated jobs atomically.
  
  ### Arguments
  - `db`: Database connection
  - `batch`: Batch map containing id, callback-fn-sym, linger-sec, queue, ready-queue, retry-opts, jobs, total, status, created-at
  - `opts`: Options map containing table names
  
  ### Returns
  The batch map"
  [db batch opts]
  (let [{:keys [id callback-fn-sym linger-sec queue ready-queue retry-opts jobs total status created-at]} batch
        batches-table (table-name opts :batches)
        batch-jobs-table (table-name opts :batch-jobs)
        serialized-retry-opts (serialize-job-data retry-opts)]
    (jdbc/with-transaction [tx db]
      (jdbc/execute! tx
                     [(str "INSERT INTO " batches-table " (id, callback_fn_sym, linger_sec, queue, ready_queue, retry_opts, total, status, created_at)"
                           " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                      id (str callback-fn-sym) linger-sec queue ready-queue serialized-retry-opts total (name status) created-at])

      (doseq [job jobs]
        (jdbc/execute! tx
                       [(str "INSERT INTO " batch-jobs-table " (job_id, batch_id, status) VALUES (?, ?, ?)")
                        (:id job) id "enqueued"]))

      (doseq [job jobs]
        (enqueue-job! tx job opts)))
    batch))

(defn get-batch-status
  "Gets the current status of a batch including job counts.
  
  ### Arguments
  - `db`: Database connection
  - `batch-id`: Batch ID
  - `opts`: Options map containing table names
  
  ### Returns
  Map with batch info and job counts or nil if not found"
  [db batch-id opts]
  (jdbc/with-transaction [tx db]
    (when-let [batch-row (first (jdbc/execute! tx
                                               [(str "SELECT * FROM " (table-name opts :batches) " WHERE id = ?") batch-id]
                                               {:builder-fn rs/as-unqualified-lower-maps}))]
      (let [job-counts (jdbc/execute! tx
                                      [(str "SELECT status, COUNT(*) as count FROM " (table-name opts :batch-jobs) " WHERE batch_id = ? GROUP BY status")
                                       batch-id]
                                      {:builder-fn rs/as-unqualified-lower-maps})
            counts-map (into {} (map (fn [row] [(:status row) (:count row)]) job-counts))]
        (-> batch-row
            (update :callback_fn_sym #(when % (symbol %)))
            (update :retry_opts deserialize-job-data)
            (update :status keyword)
            (assoc :enqueued (get counts-map "enqueued" 0)
                   :retrying (get counts-map "retrying" 0)
                   :success (get counts-map "success" 0)
                   :dead (get counts-map "dead" 0)))))))

(defn update-batch-job-status!
  "Updates the status of a job within a batch and returns updated batch status.
  
  ### Arguments
  - `db`: Database connection
  - `batch-id`: Batch ID
  - `job-id`: Job ID
  - `new-status`: New status for the job (\"success\", \"dead\", \"retrying\")
  - `opts`: Options map containing table names
  
  ### Returns
  Updated batch status keyword or nil if batch not found"
  [db batch-id job-id new-status opts]
  (jdbc/with-transaction [tx db]
    (jdbc/execute! tx
                   [(str "UPDATE " (table-name opts :batch-jobs) " SET status = ?, completed_at = ? WHERE job_id = ? AND batch_id = ?")
                    new-status (when (#{:success :dead} (keyword new-status)) (u/epoch-time-ms)) job-id batch-id])

    (when-let [batch-info (get-batch-status tx batch-id opts)]
      (let [{:keys [enqueued retrying success dead]} batch-info
            batch-status (cond
                           (< 0 (+ enqueued retrying)) :in-progress
                           (= 0 dead) :success
                           (= 0 success) :dead
                           :else :partial-success)]

        (when (#{:success :dead :partial-success} batch-status)
          (jdbc/execute! tx
                         [(str "UPDATE " (table-name opts :batches) " SET status = ?, completed_at = ? WHERE id = ?")
                          (name batch-status) (u/epoch-time-ms) batch-id]))

        batch-status))))

(defn cleanup-completed-batch!
  "Marks a completed batch for cleanup.
  
  ### Arguments
  - `db`: Database connection
  - `batch-id`: Batch ID
  - `linger-sec`: Seconds to keep the batch before cleanup (not used in current implementation)
  - `opts`: Options map containing table names
  
  ### Returns
  Number of rows affected"
  [db batch-id _linger-sec opts]
  (jdbc/execute! db
                 [(str "UPDATE " (table-name opts :batches) " SET completed_at = ? WHERE id = ? AND completed_at IS NULL")
                  (u/epoch-time-ms) batch-id]))

(defn delete-batch!
  "Deletes a batch and all its associated jobs and tracking records.
  
  ### Arguments
  - `db`: Database connection
  - `batch-id`: Batch ID
  - `opts`: Options map containing table names
  
  ### Returns
  Number of batches deleted"
  [db batch-id opts]
  (jdbc/with-transaction [tx db]
    (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :enqueued-jobs) " WHERE batch_id = ?") batch-id])
    (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :scheduled-jobs) " WHERE batch_id = ?") batch-id])
    (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :dead-jobs) " WHERE batch_id = ?") batch-id])

    (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :batch-jobs) " WHERE batch_id = ?") batch-id])

    (-> (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :batches) " WHERE id = ?") batch-id])
        first
        :next.jdbc/update-count)))

(defn replay-dead-job!
  "Moves a dead job back to the ready queue for re-execution.
  
  ### Arguments
  - `db`: Database connection
  - `job`: Dead job to replay
  - `opts`: Options map containing table names
  
  ### Returns
  The replayed job"
  [db job opts]
  (let [dead-jobs-table (table-name opts :dead-jobs)]
    (jdbc/with-transaction [tx db]
      (jdbc/execute! tx [(str "DELETE FROM " dead-jobs-table " WHERE id = ?") (:id job)])

      (let [clean-job (dissoc job :died-at :exception-info)]
        (enqueue-job! tx clean-job opts)
        clean-job))))

(defn pop-dead-job!
  "Pops (removes and returns) the oldest dead job.
  
  ### Arguments
  - `db`: Database connection
  - `opts`: Options map containing table names
  
  ### Returns
  Dead job map or nil if no dead jobs exist"
  [db opts]
  (let [dead-jobs-table (table-name opts :dead-jobs)]
    (jdbc/with-transaction [tx db]
      (when-let [job-row (first (jdbc/execute! tx
                                               [(str "SELECT * FROM " dead-jobs-table " ORDER BY died_at ASC LIMIT 1")]
                                               {:builder-fn rs/as-unqualified-lower-maps}))]
        (jdbc/execute! tx [(str "DELETE FROM " dead-jobs-table " WHERE id = ?") (:id job-row)])

        (-> job-row
            (update :execute_fn_sym #(when % (symbol %)))
            (update :args deserialize-job-data)
            (update :retry_opts deserialize-job-data)
            (update :exception_trace deserialize-job-data))))))

(defn replay-n-dead-jobs!
  "Replays up to n dead jobs by moving them back to ready queue.
  
  ### Arguments
  - `db`: Database connection
  - `n`: Maximum number of jobs to replay
  - `opts`: Options map containing table names
  
  ### Returns
  Number of jobs replayed"
  [db n opts]
  (let [dead-jobs-table (table-name opts :dead-jobs)]
    (jdbc/with-transaction [tx db]
      (let [dead-jobs (jdbc/execute! tx
                                     [(str "SELECT * FROM " dead-jobs-table " ORDER BY died_at ASC LIMIT ?") n]
                                     {:builder-fn rs/as-unqualified-lower-maps})]
        (doseq [job-row dead-jobs]
          (let [job (-> job-row
                        (update :execute_fn_sym #(when % (symbol %)))
                        (update :args deserialize-job-data)
                        (update :retry_opts deserialize-job-data)
                        (dissoc :died_at :exception_type :exception_message :exception_trace))]
            (jdbc/execute! tx [(str "DELETE FROM " dead-jobs-table " WHERE id = ?") (:id job)])

            (enqueue-job! tx job opts)))

        (count dead-jobs)))))

(defn delete-dead-jobs-older-than!
  "Deletes dead jobs older than the specified timestamp.
  
  ### Arguments
  - `db`: Database connection
  - `epoch-ms`: Epoch timestamp in milliseconds
  - `opts`: Options map containing table names
  
  ### Returns
  Number of jobs deleted"
  [db epoch-ms opts]
  (let [dead-jobs-table (table-name opts :dead-jobs)]
    (-> (jdbc/execute! db [(str "DELETE FROM " dead-jobs-table " WHERE died_at < ?") epoch-ms])
        first
        :next.jdbc/update-count)))

(defn prioritise-job-execution!
  "Moves a job to the front of its queue by setting high priority.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table name (:enqueued-jobs or :scheduled-jobs)
  - `job-id`: Job ID to prioritize
  - `opts`: Options map containing table names
  
  ### Returns
  Number of rows affected"
  [db table job-id opts]
  (let [table-name-val (table-name opts table)
        max-priority-query (str "SELECT COALESCE(MAX(priority), 0) + 1 as max_priority FROM " table-name-val)
        max-priority (-> (jdbc/execute! db [max-priority-query])
                         first
                         :max_priority)]
    (-> (jdbc/execute! db [(str "UPDATE " table-name-val " SET priority = ? WHERE id = ?") max-priority job-id])
        first
        :next.jdbc/update-count)))

(defn move-scheduled-job-to-ready!
  "Immediately moves a scheduled job to the ready queue.
  
  ### Arguments
  - `db`: Database connection
  - `job-id`: Scheduled job ID to move
  - `opts`: Options map containing table names
  
  ### Returns
  Number of jobs moved"
  [db job-id opts]
  (jdbc/with-transaction [tx db]
    (when-let [scheduled-job (first (jdbc/execute! tx
                                                   [(str "SELECT * FROM " (table-name opts :scheduled-jobs) " WHERE id = ?") job-id]
                                                   {:builder-fn rs/as-unqualified-lower-maps}))]
      (let [job (-> scheduled-job
                    (update :execute_fn_sym #(when % (symbol %)))
                    (update :args deserialize-job-data)
                    (update :retry_opts deserialize-job-data)
                    (dissoc :scheduled_at))]
        (enqueue-job! tx job opts)

        (jdbc/execute! tx [(str "DELETE FROM " (table-name opts :scheduled-jobs) " WHERE id = ?") job-id])
        1))))

(defn list-all-queues
  "Lists all unique queue names from enqueued jobs.
  
  ### Arguments
  - `db`: Database connection
  - `opts`: Options map containing table names
  
  ### Returns
  Vector of queue names"
  [db opts]
  (->> (jdbc/execute! db [(str "SELECT DISTINCT queue FROM " (table-name opts :enqueued-jobs) " ORDER BY queue")]
                      {:builder-fn rs/as-unqualified-lower-maps})
       (map :queue)
       vec))

(defn find-jobs-by-pattern
  "Finds jobs matching a pattern in their serialized data.
  
  ### Arguments
  - `db`: Database connection
  - `table`: Table name (:enqueued-jobs, :scheduled-jobs, :dead-jobs)
  - `match-fn`: Function that takes a job map and returns true if it matches
  - `limit`: Maximum number of results
  - `opts`: Options map containing table names
  
  ### Returns
  Vector of matching job maps"
  [db table match-fn limit opts]
  (let [table-name-val (table-name opts table)
        all-jobs (jdbc/execute! db [(str "SELECT * FROM " table-name-val " LIMIT ?") limit]
                                {:builder-fn rs/as-unqualified-lower-maps})]
    (->> all-jobs
         (map (fn [job-row]
                (-> job-row
                    (update :execute_fn_sym #(when % (symbol %)))
                    (update :args deserialize-job-data)
                    (update :retry_opts deserialize-job-data))))
         (filter match-fn)
         vec)))