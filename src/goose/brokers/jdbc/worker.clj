(ns ^:no-doc goose.brokers.jdbc.worker
  "JDBC worker implementation for processing jobs."
  (:require
   [clojure.tools.logging :as log]
   [com.climate.claypoole :as cp]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :as conn]
   [goose.consumer :as consumer]
   [goose.defaults :as d]
   [goose.utils :as u]
   [goose.worker])
  (:import
   [java.sql Connection]
   [java.util.concurrent TimeUnit]))

(defn- generate-worker-id
  "Generates a unique worker ID"
  []
  (str (random-uuid)))

(defn- internal-stop
  "Stops the worker internal components."
  [worker-state]
  (when-let [pool (:thread-pool @worker-state)]
    (log/info "Shutting down JDBC worker thread pool")
    (cp/shutdown pool)))

(defn- chain-middlewares
  "Chains middleware functions around the job execution."
  [middlewares]
  (reduce (fn [next-middleware current-middleware]
            (current-middleware next-middleware))
          consumer/execute-job
          (reverse middlewares)))

(defn- poll-and-execute-jobs
  "Polls for jobs and executes them."
  [{:keys [data-source ready-queue call thread-pool] :as opts}]
  (u/while-pool
   thread-pool
   (u/log-on-exceptions
    (conn/with-connection data-source
      (fn [^Connection conn]
        (when-let [job (cmd/dequeue-job! conn ready-queue opts)]
          (log/debug "Processing job:" (:id job))
          (call opts job)))))))

(defn- schedule-jobs-poller
  "Polls for scheduled jobs that are due and moves them to ready queue."
  [{:keys [data-source thread-pool] :as opts}]
  (cp/future
    thread-pool
    (loop []
      (try
        (let [moved (cmd/move-scheduled-jobs-to-ready! data-source opts)]
          (when (pos? moved)
            (log/debug "Moved" moved "scheduled jobs to ready queue")))
        (catch Exception e
          (log/warn e "Error processing scheduled jobs")))

      (Thread/sleep (* 5 1000))

      (when-not (.isInterrupted (Thread/currentThread))
        (recur)))))

(defn start
  "Starts a JDBC worker with the given options.
  
  ### Options
  - `:data-source` - JDBC connection manager
  - `:threads` - Number of worker threads (default: 1)
  - `:queue` - Queue name to process (default: 'default')
  - `:middlewares` - Middleware functions for job execution
  - `:auto-scheduler?` - Whether to run scheduled job poller (default: true)
  - `:graceful-shutdown-sec` - Seconds to wait for graceful shutdown (default: 30)
  
  ### Returns
  Worker map that implements the goose.worker/Shutdown protocol"
  [{:keys [data-source threads queue middlewares auto-scheduler? graceful-shutdown-sec]
    :or {threads 1
         queue "default"
         middlewares []
         auto-scheduler? true
         graceful-shutdown-sec 30}
    :as opts}]

  (let [worker-id (generate-worker-id)
        pool-opts {:cpus threads
                   :queue-length 1000
                   :thread-name "goose-jdbc-worker"}
        thread-pool (cp/threadpool threads pool-opts)
        ready-queue (d/prefix-queue queue)
        call (chain-middlewares middlewares)
        worker-state (atom {:thread-pool thread-pool
                            :data-source data-source
                            :ready-queue ready-queue
                            :worker-id worker-id})

        worker-opts (assoc opts
                           :thread-pool thread-pool
                           :ready-queue ready-queue
                           :call call
                           :worker-id worker-id)

        consumer-futures (doall
                          (repeatedly threads
                                      #(cp/future thread-pool
                                                  (poll-and-execute-jobs worker-opts))))

        scheduler-future (when auto-scheduler?
                           (log/info "Starting scheduled job poller for worker " worker-id)
                           (schedule-jobs-poller worker-opts))]

    (log/info (str "Started JDBC worker with ID: " worker-id " with " threads " threads processing queue: " ready-queue))

    (vary-meta
     {:state worker-state
      :consumer-futures consumer-futures
      :scheduler-future scheduler-future
      :graceful-shutdown-sec graceful-shutdown-sec}
     assoc
     `goose.worker/stop
     (fn [this]
       (log/info "Stopping JDBC worker")
       (internal-stop (:state this))
       (doseq [future (:consumer-futures this)]
         (future-cancel future))
       (when-let [scheduler-future (:scheduler-future this)]
         (future-cancel scheduler-future))
       (when-let [pool (:thread-pool @(:state this))]
         (cp/shutdown pool)
         (let [completed? (.awaitTermination pool (:graceful-shutdown-sec this) TimeUnit/SECONDS)]
           (if completed?
             (log/info "JDBC worker stopped gracefully")
             (do
               (log/warn "JDBC worker graceful shutdown timeout, forcing shutdown")
               (cp/shutdown! pool)
               (log/info "JDBC worker forcefully stopped")))))))))
