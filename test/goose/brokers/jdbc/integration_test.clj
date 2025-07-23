(ns goose.brokers.jdbc.integration-test
  "Comprehensive integration tests for JDBC broker across multiple databases."
  (:require
   [clojure.java.shell]
   [clojure.test :refer [deftest is testing]]
   [clojure.tools.logging :as log]
   [goose.batch :as batch]
   [goose.broker :as b]
   [goose.brokers.jdbc.broker :as jdbc-broker]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :as conn]
   [goose.brokers.jdbc.test-utils :as test-utils]
   [goose.client :as c]
   [goose.defaults :as d]
   [goose.job :as j]
   [goose.retry :as retry]
   [goose.utils :as u]
   [goose.worker :as worker]
   [next.jdbc.connection :as jdbc-conn])
  (:import
   [com.zaxxer.hikari HikariDataSource]
   [java.sql Connection]))

(def ^:private test-databases [:sqlite :postgresql :mysql])

;; Single atom to hold current test database info
(def ^:private current-db (atom nil))

(def ^:private db-specs
  {:postgresql {:dbtype "postgresql"
                :dbname "goose_test"
                :host "localhost"
                :port 5432
                :username "goose"
                :password "goose"
                :maximumPoolSize 2}
   :mysql {:dbtype "mysql"
           :dbname "goose_test"
           :host "localhost"
           :port 3306
           :username "goose"
           :password "goose"
           :allowMultiQueries true
           :maximumPoolSize 2}})

(defn- setup-database!
  [db-type]
  (if (= :sqlite db-type)
    ;; sqlite is a special case and always gets a fresh database file
    (let [{:keys [data-source db-file]} (test-utils/create-sqlite-test-db)]
      (reset! current-db {:db-type db-type
                          :data-source data-source
                          :db-file db-file}))
    ;; for others we create fresh ds pool
    (let [db-spec (get db-specs db-type)
          data-source (jdbc-conn/->pool HikariDataSource db-spec)]
      (case db-type
        :postgresql (test-utils/setup-postgresql-schema data-source)
        :mysql (test-utils/setup-mysql-schema data-source "goose_test"))
      (reset! current-db {:db-type db-type
                          :data-source data-source
                          :db-file nil}))))

(defn- cleanup-database
  []
  (when-let [{:keys [data-source db-file]} @current-db]
    (when data-source
      (try (.close data-source) (catch Exception _)))
    (when db-file
      (test-utils/cleanup-sqlite-file db-file)))
  (reset! current-db nil))

(defn with-database
  "Runs test function with a single database."
  [db-type test-fn]
  (try
    (setup-database! db-type)
    (let [{:keys [data-source]} @current-db
          producer (jdbc-broker/new-producer {:data-source data-source})]
      (try
        (when-not (= :sqlite db-type) ;; sqlite file is always fresh, no need to clean
          (test-utils/truncate-all-tables! producer))
        (test-fn db-type producer)
        (finally
          (when-not (= :sqlite db-type)
            (try
              (test-utils/truncate-all-tables! producer)
              (catch Exception e
                (log/debug "Failed to clean database after test" e))))
          (.close producer))))
    (finally
      (cleanup-database))))

(defn with-all-databases
  "Runs test function with all available databases."
  [test-fn]
  (doseq [db-type test-databases]
    (testing (str "Testing with " db-type)
      (with-database db-type test-fn))))

(def job-executed (atom false))

(defn test-job-fn [arg]
  (reset! job-executed true)
  (str "processed: " arg))

(deftest ^:integration multi-database-operations-test
  (testing "Basic broker operations work across all available databases"
    (with-all-databases
      (fn [_db-type producer]
        (reset! job-executed false)
        (let [job-data (j/new `test-job-fn (list "test") d/default-queue d/default-queue {:max-retries 3})]

          (b/enqueue producer job-data)
          (is (= 1 (b/enqueued-jobs-size producer d/default-queue)))

          (is (some? (b/enqueued-jobs-find-by-id producer d/default-queue (:id job-data))))

          (let [scheduled-job (j/new `test-job-fn (list "scheduled") d/default-queue d/default-queue {:max-retries 3})
                future-time (+ (System/currentTimeMillis) 60000)]
            (b/schedule producer future-time scheduled-job)
            (is (= 1 (b/scheduled-jobs-size producer))))

          (b/enqueued-jobs-purge producer d/default-queue)
          (is (= 0 (b/enqueued-jobs-size producer d/default-queue)))

          (b/scheduled-jobs-purge producer)
          (is (= 0 (b/scheduled-jobs-size producer))))))))

(deftest ^:integration dequeue-operations-test
  (testing "Dequeue operations work"
    (with-all-databases
      (fn [_db-type producer]
        (let [queue "dequeue-test"
              ready-queue (d/prefix-queue queue)
              job-count 5
              jobs (for [i (range job-count)]
                     (j/new `test-job-fn (list (str "job-" i)) queue ready-queue {:max-retries 3}))]

          (doseq [job jobs]
            (b/enqueue producer job))

          (is (= job-count (b/enqueued-jobs-size producer queue)))

          (let [dequeued-jobs (conn/with-connection (-> producer :opts :data-source)
                                (fn [^Connection conn]
                                  (let [opts (:opts producer)]
                                    (doall (repeatedly (+ job-count 1) #(cmd/dequeue-job! conn ready-queue opts))))))
                valid-jobs (filter some? dequeued-jobs)]

            ;; should dequeue exactly job-count jobs
            (is (= job-count (count valid-jobs)))
            ;; queue should be empty
            (is (= 0 (b/enqueued-jobs-size producer queue)))))))))

(defn test-fn [arg]
  (str "processed: " arg))

(deftest jdbc-client-api-test
  (testing "JDBC broker works with client API"
    (with-all-databases
      (fn [_db-type broker]
        (testing "perform-async"
          (let [result (c/perform-async {:broker broker
                                         :queue "test-queue"
                                         :retry-opts retry/default-opts}
                                        `test-fn "hello")]
            (is (string? (:id result)))
            (is (= 1 (b/enqueued-jobs-size broker "test-queue")))))

        (testing "perform-in-sec"
          (let [result (c/perform-in-sec {:broker broker
                                          :queue "test-queue"
                                          :retry-opts retry/default-opts}
                                         60 `test-fn "delayed")]
            (is (string? (:id result)))
            (is (= 1 (b/scheduled-jobs-size broker)))))
        (testing "perform-at"
          (let [future-instant (java.time.Instant/ofEpochMilli (+ (System/currentTimeMillis) 120000))
                result (c/perform-at {:broker broker
                                      :queue "test-queue"
                                      :retry-opts retry/default-opts}
                                     future-instant `test-fn "scheduled")]
            (is (string? (:id result)))
            (is (= 2 (b/scheduled-jobs-size broker)))))))))

(deftest ^:integration broker-basic-operations-test
  (testing "Basic broker operations across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [test-job (j/new 'println ["Hello" "Broker"] "default" "default" {:max-retries 3})]

          ;; Test enqueue - returns {:id "..."}
          (let [enqueued-result (b/enqueue broker test-job)]
            (is (= (:id test-job) (:id enqueued-result)))
            (is (= 1 (count (keys enqueued-result)))) ; Should only have :id key
            (is (contains? enqueued-result :id)))

          ;; Test job count
          (is (= 1 (b/enqueued-jobs-size broker "default")))

          ;; Test find by ID
          (let [found-job (b/enqueued-jobs-find-by-id broker "default" (:id test-job))]
            (is found-job)
            (is (= (:id test-job) (:id found-job))))

          ;; Test delete
          (is (= 1 (b/enqueued-jobs-delete broker test-job)))
          (is (= 0 (b/enqueued-jobs-size broker "default"))))))))

(deftest ^:integration broker-scheduled-jobs-test
  (testing "Scheduled job operations across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [future-time (+ (u/epoch-time-ms) 120000)
              test-job (j/new 'println ["Scheduled" "Job"] "default" "default" {:max-retries 3})]

          ;; Test schedule - returns {:id "..."}
          (let [scheduled-result (b/schedule broker future-time test-job)]
            (is (= (:id test-job) (:id scheduled-result))))

          ;; Test scheduled job count
          (is (= 1 (b/scheduled-jobs-size broker)))

          ;; Test find scheduled job
          (let [found-job (b/scheduled-jobs-find-by-id broker (:id test-job))]
            (is found-job)
            (is (= (:id test-job) (:id found-job))))

          ;; Test delete scheduled job
          (is (= 1 (b/scheduled-jobs-delete broker test-job)))
          (is (= 0 (b/scheduled-jobs-size broker))))))))

(deftest ^:integration broker-cron-jobs-test
  (testing "Cron job operations across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [cron-opts {:name "test-cron" :cron "0 0 * * *"}
              job-description {:execute-fn-sym 'println
                               :args ["Cron" "Job"]
                               :queue "default"
                               :ready-queue "default"
                               :retry-opts {:max-retries 3}}]

          ;; Test register cron
          (let [cron-entry (b/register-cron broker cron-opts job-description)]
            (is (= "test-cron" (:name cron-entry)))
            (is (= "0 0 * * *" (:cron-expression cron-entry))))

          ;; Test cron job count
          (is (= 1 (b/cron-jobs-size broker)))

          ;; Test find cron job
          (let [found-cron (b/cron-jobs-find-by-name broker "test-cron")]
            (is found-cron))

          ;; Test delete cron job
          (is (= 1 (b/cron-jobs-delete broker "test-cron")))
          (is (= 0 (b/cron-jobs-size broker))))))))

(deftest ^:integration broker-dead-jobs-test
  (testing "Dead job operations across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [dead-job {:id (str (random-uuid))
                        :queue "default"
                        :ready-queue "default"
                        :execute-fn-sym 'println
                        :args ["Dead" "Job"]
                        :retry-opts {:max-retries 0}
                        :enqueued-at (u/epoch-time-ms)}]

          ;; Manually insert a dead job (simulating job failure)
          ;; In real scenarios, this would happen through the retry mechanism
          (conn/with-connection (:data-source (:opts broker))
            (fn [^Connection conn]
              (let [exception-info {:type "RuntimeException"
                                    :message "Test failure"
                                    :trace ["trace1" "trace2"]}]
                (cmd/move-job-to-dead! conn dead-job exception-info (:opts broker)))))

          ;; Test dead job count
          (is (= 1 (b/dead-jobs-size broker)))

          ;; Test find dead job
          (let [found-job (b/dead-jobs-find-by-id broker (:id dead-job))]
            (is found-job)
            (is (= (:id dead-job) (:id found-job))))

          ;; Test delete dead job
          (is (= 1 (b/dead-jobs-delete broker dead-job)))
          (is (= 0 (b/dead-jobs-size broker))))))))

(deftest ^:integration broker-purge-operations-test
  (testing "Purge operations for all job types across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [test-job1 (j/new 'println ["Job" "1"] "test-queue" "test-queue" {:max-retries 3})
              test-job2 (j/new 'println ["Job" "2"] "test-queue" "test-queue" {:max-retries 3})
              future-time (+ (u/epoch-time-ms) 120000)]

          ;; Add jobs to different queues
          (b/enqueue broker test-job1)
          (b/enqueue broker test-job2)
          (b/schedule broker future-time (assoc test-job1 :id (str (random-uuid))))

          ;; Verify jobs exist
          (is (= 2 (b/enqueued-jobs-size broker "test-queue")))
          (is (= 1 (b/scheduled-jobs-size broker)))

          ;; Test purge operations
          (is (= 2 (b/enqueued-jobs-purge broker "test-queue")))
          (is (= 0 (b/enqueued-jobs-size broker "test-queue")))

          (is (= 1 (b/scheduled-jobs-purge broker)))
          (is (= 0 (b/scheduled-jobs-size broker))))))))

(deftest ^:integration worker-integration-test
  (testing "Worker integration with broker across all databases"
    (with-all-databases
      (fn [_db-type broker]
        (let [results (atom [])
              test-fn (fn [msg]
                        (swap! results conj msg)
                        (Thread/sleep 100))]

          ;; Define the test function in this namespace
          (intern 'goose.brokers.jdbc.integration-test 'test-worker-fn test-fn)

          (let [test-job (j/new 'goose.brokers.jdbc.integration-test/test-worker-fn
                                ["Worker test"]
                                "worker-queue"
                                "goose/queue:worker-queue"
                                {:max-retries 3})]

            ;; Enqueue job
            (b/enqueue broker test-job)
            (is (= 1 (b/enqueued-jobs-size broker "worker-queue")))

            ;; Start worker
            (let [worker (b/start-worker broker {:threads 1
                                                 :queue "worker-queue"
                                                 :auto-scheduler? false})]
              (try
                ;; Wait for job to be processed
                (Thread/sleep 1000)

                ;; Verify job was processed
                (is (= 0 (b/enqueued-jobs-size broker "worker-queue")))
                (is (= ["Worker test"] @results))

                (finally
                  (worker/stop worker))))))))))

(deftest ^:integration worker-id-test
  (testing "JDBC workers have worker IDs"
    (with-all-databases
      (fn [_db-type broker]
        (let [worker (b/start-worker broker {:threads 1
                                             :queue "test-queue"
                                             :auto-scheduler? false})]
          (try
            ;; Check that worker has an ID in its state
            (let [worker-id (-> worker :state deref :worker-id)]
              (is (some? worker-id) "Worker should have an ID"))

            (finally
              (worker/stop worker))))))))

(deftest ^:integration batch-operations-test
  (testing "Batch operations across all databases"
    (with-all-databases
      (fn [_db-type broker]
        ;; Test batch creation and enqueue
        (let [job1 (j/new 'println ["Batch" "Job" "1"] "batch-queue" "batch-queue" {:max-retries 3})
              job2 (j/new 'println ["Batch" "Job" "2"] "batch-queue" "batch-queue" {:max-retries 3})
              jobs [job1 job2]
              batch-opts {:callback-fn-sym 'goose.batch/default-callback :linger-sec 3600}
              batch (batch/new {:queue "batch-queue" :ready-queue "batch-queue" :retry-opts {:max-retries 3}}
                               batch-opts jobs)]

          ;; Test batch structure
          (is (string? (:id batch)))
          (is (= 2 (:total batch)))
          (is (= :in-progress (:status batch)))

          ;; Test batch enqueue
          (let [enqueued-batch (b/enqueue-batch broker batch)]
            (is (= (:id batch) (:id enqueued-batch))))

          ;; Test that jobs were enqueued
          (is (= 2 (b/enqueued-jobs-size broker "batch-queue")))

          ;; Test batch status
          (let [status (b/batch-status broker (:id batch))]
            (is status)
            (is (= (:id batch) (:id status)))
            (is (= :in-progress (:status status)))
            (is (= 2 (:total status)))
            (is (= 2 (:enqueued status))))

          ;; Test batch job tracking
          (conn/with-connection (:data-source (:opts broker))
            (fn [^Connection conn]
              ;; Mark first job as successful
              (let [new-status (cmd/update-batch-job-status! conn (:id batch) (:id job1) "success" (:opts broker))]
                (is (= :in-progress new-status)))

              ;; Mark second job as successful (should complete batch)
              (let [new-status (cmd/update-batch-job-status! conn (:id batch) (:id job2) "success" (:opts broker))]
                (is (= :success new-status)))))

          ;; Verify final status
          (let [final-status (b/batch-status broker (:id batch))]
            (is (= :success (:status final-status)))
            (is (= 0 (:enqueued final-status)))
            (is (= 2 (:success final-status))))

          ;; Test batch deletion
          (let [deleted-count (b/batch-delete broker (:id batch))]
            (is (= 1 deleted-count)))
          (is (nil? (b/batch-status broker (:id batch)))))))))
