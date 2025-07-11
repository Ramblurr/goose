(ns ^:no-doc goose.brokers.jdbc.batch
  "JDBC batch operations and middleware for the JDBC broker."
  (:require
   [clojure.tools.logging :as log]
   [goose.batch :as batch]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :as conn]
   [goose.metrics :as m]
   [goose.retry]
   [goose.utils :as u])
  (:import
   [java.sql Connection]))

(defn enqueue
  "Enqueues a batch of jobs atomically.
  
  ### Arguments
  - `data-source`: JDBC connection manager
  - `batch`: Batch map containing id, jobs, callback-fn-sym, linger-sec, etc.
  - `opts`: Options map containing table names
  
  ### Returns
  The batch map"
  [data-source batch opts]
  (conn/with-connection data-source
    (fn [^Connection conn]
      (cmd/enqueue-batch! conn batch opts))))

(defn get-batch
  "Gets the current status of a batch.
  
  ### Arguments
  - `data-source`: JDBC connection manager
  - `batch-id`: Batch ID
  - `opts`: Options map containing table names
  
  ### Returns
  Batch status map or nil if not found"
  [data-source batch-id opts]
  (conn/with-connection data-source
    (fn [^Connection conn]
      (cmd/get-batch-status conn batch-id opts))))

(defn- record-metrics
  "Records batch completion metrics."
  [{:keys [metrics-plugin]}
   {:keys [execute-fn-sym]}
   {:keys [id queue created-at]}
   completion-status]
  (when (m/enabled? metrics-plugin)
    (let [completion-time (- (u/epoch-time-ms) created-at)
          tags {:batch-id id :execute-fn-sym execute-fn-sym :queue queue}]
      (m/increment metrics-plugin (m/format-batch-status completion-status) 1 tags)
      (m/timing metrics-plugin m/batch-completion-time completion-time tags))))

(defn- enqueue-callback-and-cleanup-batch
  "Enqueues callback job and marks batch for cleanup when completed."
  [data-source
   {:keys [id linger-sec] :as batch}
   completion-status
   opts]
  (conn/with-connection data-source
    (fn [^Connection conn]
      (let [callback (batch/new-callback-job batch id completion-status)]
        (cmd/enqueue-job! conn callback opts)

        (cmd/cleanup-completed-batch! conn id linger-sec opts)))))

(defn- mark-batch-completion
  "Marks a batch as completed and handles callback/cleanup."
  [{:keys [data-source] :as opts} job batch-id completion-status]
  (if-let [batch (get-batch data-source batch-id opts)]
    (do (enqueue-callback-and-cleanup-batch data-source batch completion-status opts)
        (record-metrics opts job batch completion-status))
    (log/warnf "Job executed after batch-id: %s was deleted." batch-id)))

(defn- execute-batch
  "Executes a batch job and updates batch state."
  [next
   {:keys [data-source] :as opts}
   {job-id :id batch-id :batch-id :as job}]
  (let [status (atom nil)]
    (try
      (let [response (next opts job)]
        (conn/with-connection data-source
          (fn [^Connection conn]
            (reset! status (cmd/update-batch-job-status! conn batch-id job-id "success" opts))))
        response)
      (catch Exception ex
        (let [failed-job (goose.retry/set-failed-config job ex)
              new-status (if (goose.retry/max-retries-reached? failed-job)
                           "dead"
                           "retrying")]
          (conn/with-connection data-source
            (fn [^Connection conn]
              (reset! status (cmd/update-batch-job-status! conn batch-id job-id new-status opts))))
          (throw ex)))
      (finally
        (when (and @status (batch/terminal-state? @status))
          (mark-batch-completion opts job batch-id @status))))))

(defn wrap-state-update
  "Middleware wrapper that handles batch job state updates.
  
  This is similar to the Redis batch middleware but uses JDBC operations
  to track job state within batches."
  [next]
  (fn [opts {:keys [batch-id] :as job}]
    (if batch-id
      (execute-batch next opts job)
      (next opts job))))