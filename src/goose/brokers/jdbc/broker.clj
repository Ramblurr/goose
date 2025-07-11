(ns goose.brokers.jdbc.broker
  "JDBC broker implementation that implements the Broker protocol."
  (:require
   [clojure.tools.logging :as log]
   [goose.broker :as b]
   [goose.brokers.jdbc.batch :as jdbc-batch]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :as conn]
   [goose.brokers.jdbc.worker :as worker])
  (:import
   [java.io Closeable]
   [java.sql Connection]))

(defrecord JDBCBroker [opts]
  b/Broker

  (enqueue [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/enqueue-job! conn job opts)
        (select-keys job [:id]))))

  (enqueue-batch [_this batch]
    (jdbc-batch/enqueue (:data-source opts) batch opts)
    (select-keys batch [:id]))

  (schedule [_this schedule-epoch-ms job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/schedule-job! conn schedule-epoch-ms job opts)
        (select-keys job [:id]))))

  (register-cron [_this cron-opts job-description]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (let [cron-entry (assoc job-description
                                :name (:name cron-opts)
                                :cron-expression (:cron cron-opts))]
          (cmd/register-cron-job! conn cron-entry opts)))))

  (start-worker [_this worker-opts]
    (let [worker-options (merge worker-opts opts {:data-source (:data-source opts)})]
      (worker/start worker-options)))

  (enqueued-jobs-list-all-queues [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/list-all-queues conn opts))))

  (enqueued-jobs-size [_this queue]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/count-jobs conn :enqueued-jobs opts queue))))

  (enqueued-jobs-find-by-id [_this _queue id]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-job-by-id conn :enqueued-jobs id opts))))

  (enqueued-jobs-find-by-pattern [_this _queue match? limit]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-jobs-by-pattern conn :enqueued-jobs match? limit opts))))

  (enqueued-jobs-prioritise-execution [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/prioritise-job-execution! conn :enqueued-jobs (:id job) opts))))

  (enqueued-jobs-delete [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-job-by-id! conn :enqueued-jobs (:id job) opts))))

  (enqueued-jobs-purge [_this queue]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/purge-table! conn :enqueued-jobs opts queue))))

  (scheduled-jobs-size [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/count-jobs conn :scheduled-jobs opts))))

  (scheduled-jobs-find-by-id [_this id]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-job-by-id conn :scheduled-jobs id opts))))

  (scheduled-jobs-find-by-pattern [_this match? limit]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-jobs-by-pattern conn :scheduled-jobs match? limit opts))))

  (scheduled-jobs-prioritise-execution [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/move-scheduled-job-to-ready! conn (:id job) opts))))

  (scheduled-jobs-delete [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-job-by-id! conn :scheduled-jobs (:id job) opts))))

  (scheduled-jobs-purge [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/purge-table! conn :scheduled-jobs opts))))

  (cron-jobs-size [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/count-jobs conn :cron-jobs opts))))

  (cron-jobs-find-by-name [_this entry-name]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-cron-job-by-name conn entry-name opts))))

  (cron-jobs-delete [_this entry-name]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-cron-job! conn entry-name opts))))

  (cron-jobs-purge [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/purge-table! conn :cron-jobs opts))))

  (dead-jobs-size [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/count-jobs conn :dead-jobs opts))))

  (dead-jobs-pop [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/pop-dead-job! conn opts))))

  (dead-jobs-find-by-id [_this id]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-job-by-id conn :dead-jobs id opts))))

  (dead-jobs-find-by-pattern [_this match? limit]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/find-jobs-by-pattern conn :dead-jobs match? limit opts))))

  (dead-jobs-replay-job [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/replay-dead-job! conn job opts))))

  (dead-jobs-replay-n-jobs [_this n]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/replay-n-dead-jobs! conn n opts))))

  (dead-jobs-delete [_this job]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-job-by-id! conn :dead-jobs (:id job) opts))))

  (dead-jobs-delete-older-than [_this epoch-ms]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-dead-jobs-older-than! conn epoch-ms opts))))

  (dead-jobs-purge [_this]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/purge-table! conn :dead-jobs opts))))

  (batch-status [_this id]
    (jdbc-batch/get-batch (:data-source opts) id opts))

  (batch-delete [_this id]
    (conn/with-connection (:data-source opts)
      (fn [^Connection conn]
        (cmd/delete-batch! conn id opts))))

  (handler [_this _req]
    (throw (UnsupportedOperationException. "Console handler not yet implemented for JDBC broker")))

  Closeable
  (close [_this]
    nil))

(def default-opts
  "Default options for JDBC broker."
  {:enqueued-jobs-table "enqueued_jobs"
   :scheduled-jobs-table "scheduled_jobs"
   :cron-jobs-table "cron_jobs"
   :dead-jobs-table "dead_jobs"
   :batches-table "batches"
   :batch-jobs-table "batch_jobs"})

(defn new-producer
  "Creates a new JDBC broker producer.
  
  ### Args
  - `:data-source`           JDBC data source (required)
  - `:enqueued-jobs-table`   Table name for enqueued jobs (default: \"enqueued_jobs\")
  - `:scheduled-jobs-table`  Table name for scheduled jobs (default: \"scheduled_jobs\") 
  - `:cron-jobs-table`       Table name for cron jobs (default: \"cron_jobs\")
  - `:dead-jobs-table`       Table name for dead jobs (default: \"dead_jobs\")
  - `:batches-table`         Table name for batches (default: \"batches\")
  - `:batch-jobs-table`      Table name for batch jobs (default: \"batch_jobs\")
  
  ### Usage
  ```clojure
  (new-producer {:data-source (jdbc/get-datasource ...))
  ```"
  ([opts]
   (let [merged-opts (merge default-opts opts)]
     #_(specs/assert-redis-consumer conn-opts scheduler-polling-interval-sec)
     (log/info "Created JDBC broker producer")
     (->JDBCBroker merged-opts))))

(defn new-consumer
  "Creates a new JDBC broker consumer.
  
  ### Options
  Same as [[new-producer]].
  
  ### Example
  ```clojure
  (new-consumer {:data-source (jdbc/get-datasource ...))
  ```"
  ([opts]
   (new-producer opts)))