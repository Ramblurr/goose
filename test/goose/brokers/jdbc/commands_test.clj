(ns goose.brokers.jdbc.commands-test
  "Tests for JDBC broker commands."
  (:require
   [clojure.java.shell]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [goose.brokers.jdbc.broker :as broker]
   [goose.brokers.jdbc.commands :as cmd]
   [goose.brokers.jdbc.connection :refer [with-connection]]
   [goose.brokers.jdbc.db-utils :as jdbc-utils]
   [goose.utils :as u]
   [next.jdbc :as jdbc])
  (:import
   [java.sql Connection]))

(def test-conn (atom nil))
(def test-opts (atom nil))

(defn jdbc-fixture
  "Test fixture for JDBC connection setup and teardown."
  [f]
  (let [test-db-file (str "/tmp/test-" (random-uuid) ".db")
        _ (let [schema-sql (slurp "src/goose/brokers/jdbc/schema/sqlite/tables.sql")
                result (clojure.java.shell/sh "sqlite3" test-db-file :in schema-sql)]
            (when-not (zero? (:exit result))
              (throw (Exception. (str "Failed to create SQLite schema: " (:err result))))))
        opts broker/default-opts
        ds (jdbc/get-datasource {:dbtype "sqlite"
                                 :dbname test-db-file})]
    (with-connection ds
      (fn [^Connection conn]
        (reset! test-conn conn)
        (reset! test-opts opts)
        (try
          (f)
          (finally
            (try
              (.delete (java.io.File. test-db-file))
              (catch Exception _))
            (reset! test-conn nil)
            (reset! test-opts nil)))))))

(use-fixtures :each jdbc-fixture)

(defn get-conn [] @test-conn)
(defn get-opts [] @test-opts)

(deftest enqueue-and-dequeue-job-test
  (testing "Basic job enqueue and dequeue operations"
    (let [db (get-conn)
          opts (get-opts)
          test-job {:id (str (random-uuid))
                    :queue "test-queue"
                    :ready-queue "test-queue"
                    :execute-fn-sym 'println
                    :args ["Hello" "World"]
                    :retry-opts {:max-retries 3}
                    :enqueued-at (u/epoch-time-ms)
                    :priority 1}]

      (is (= test-job (cmd/enqueue-job! db test-job opts)))

      (let [dequeued-job (cmd/dequeue-job! db "test-queue" opts)]
        (is (= (:id test-job) (:id dequeued-job)))
        (is (= (:execute-fn-sym test-job) (:execute-fn-sym dequeued-job)))
        (is (= (:args test-job) (:args dequeued-job)))
        (is (= (:queue test-job) (:queue dequeued-job)))
        (is (= (:priority test-job) (:priority dequeued-job))))

      ;; Queue should be empty now
      (is (nil? (cmd/dequeue-job! db "test-queue" opts))))))

(deftest schedule-job-test
  (testing "Job scheduling operations"
    (let [db (get-conn)
          opts (get-opts)
          future-time (+ (u/epoch-time-ms) 60000)
          test-job {:id (str (random-uuid))
                    :queue "test-queue"
                    :ready-queue "test-queue"
                    :execute-fn-sym 'println
                    :args ["Scheduled" "Job"]
                    :retry-opts {:max-retries 3}
                    :enqueued-at (u/epoch-time-ms)}]

      (is (= test-job (cmd/schedule-job! db future-time test-job opts)))

      (is (empty? (cmd/get-scheduled-jobs-due db opts (u/epoch-time-ms))))

      (let [due-jobs (cmd/get-scheduled-jobs-due db opts (+ (u/epoch-time-ms) 120000))]
        (is (= 1 (count due-jobs)))
        (is (= (:id test-job) (:id (first due-jobs))))))))

(deftest move-scheduled-jobs-to-ready-test
  (testing "Moving scheduled jobs to ready queue"
    (let [db (get-conn)
          opts (get-opts)
          current-time (u/epoch-time-ms)
          past-time (- current-time 60000)
          test-job {:id (str (random-uuid))
                    :queue "test-queue"
                    :ready-queue "test-queue"
                    :execute-fn-sym 'println
                    :args ["Past" "Job"]
                    :retry-opts {:max-retries 3}
                    :enqueued-at current-time}]

      (cmd/schedule-job! db past-time test-job opts)

      ;; move due jobs to ready queue
      (let [moved-count (cmd/move-scheduled-jobs-to-ready! db opts current-time)]
        (is (= 1 moved-count)))

      ;; job should now be in ready queue
      (let [dequeued-job (cmd/dequeue-job! db "test-queue" opts)]
        (is (= (:id test-job) (:id dequeued-job))))

      ;; scheduled jobs should be empty
      (is (empty? (cmd/get-scheduled-jobs-due db opts (+ current-time 120000)))))))

(deftest dead-job-operations-test
  (testing "Dead job creation and management"
    (let [db             (get-conn)
          opts           (get-opts)
          failed-job     {:id             (str (random-uuid))
                          :queue          "test-queue"
                          :ready-queue    "test-queue"
                          :execute-fn-sym 'println
                          :args           ["Failed" "Job"]
                          :retry-opts     {:max-retries 0}
                          :enqueued-at    (u/epoch-time-ms)}
          exception-info {:type    "RuntimeException"
                          :message "Job failed"
                          :trace   ["line1" "line2" "line3"]}
          dead-job       (cmd/move-job-to-dead! db failed-job exception-info opts)]
      (is (some? (:died-at dead-job)))
      (is (= exception-info (:exception-info dead-job))))))

(deftest cron-job-operations-test
  (testing "Cron job registration"
    (let [db (get-conn)
          opts (get-opts)
          cron-entry {:name "daily-job"
                      :cron-expression "0 0 * * *"
                      :execute-fn-sym 'println
                      :args ["Daily" "Task"]
                      :queue "cron-queue"
                      :ready-queue "cron-queue"
                      :retry-opts {:max-retries 3}}]

      ;; register cron job
      (is (= cron-entry (cmd/register-cron-job! db cron-entry opts)))

      ;; re-registering should update, a replace
      (let [updated-entry (assoc cron-entry :cron-expression "0 12 * * *")]
        (is (= updated-entry (cmd/register-cron-job! db updated-entry opts)))))))

(deftest count-and-query-operations-test
  (testing "Job counting and query operations"
    (let [db (get-conn)
          opts (get-opts)
          job1 {:id (str (random-uuid))
                :queue "queue1"
                :ready-queue "queue1"
                :execute-fn-sym 'println
                :args ["Job" "1"]
                :retry-opts {:max-retries 3}
                :enqueued-at (u/epoch-time-ms)
                :priority 1}
          job2 {:id (str (random-uuid))
                :queue "queue2"
                :ready-queue "queue2"
                :execute-fn-sym 'println
                :args ["Job" "2"]
                :retry-opts {:max-retries 3}
                :enqueued-at (u/epoch-time-ms)
                :priority 2}]

      (cmd/enqueue-job! db job1 opts)
      (cmd/enqueue-job! db job2 opts)

      (is (= 2 (cmd/count-jobs db :enqueued-jobs opts)))
      (is (= 1 (cmd/count-jobs db :enqueued-jobs opts "queue1")))
      (is (= 1 (cmd/count-jobs db :enqueued-jobs opts "queue2")))

      (let [found-job (cmd/find-job-by-id db :enqueued-jobs (:id job1) opts)]
        (is found-job)
        (is (= (:id job1) (:id found-job))))

      (is (= 1 (cmd/delete-job-by-id! db :enqueued-jobs (:id job1) opts)))
      (is (= 1 (cmd/count-jobs db :enqueued-jobs opts)))

      (is (= 1 (cmd/purge-table! db :enqueued-jobs opts)))
      (is (= 0 (cmd/count-jobs db :enqueued-jobs opts))))))

(deftest serialization-test
  (testing "Job data serialization and deserialization"
    (let [complex-data {:nested {:map "value"}
                        :vector [1 2 3]
                        :keyword :test}]

      ;; serialization round-trip
      (let [serialized (cmd/serialize-job-data complex-data)
            deserialized (cmd/deserialize-job-data serialized)]
        (is (= complex-data deserialized)))

      (is (nil? (cmd/serialize-job-data nil)))
      (is (nil? (cmd/deserialize-job-data nil))))))

(deftest table-name-conversion-test
  (testing "Table name conversion from keywords to database names"
    (let [db (get-conn)
          opts (get-opts)]
      ;; ensure our table name conversion works correctly
      ;; by testing that we can successfully query tables with hyphenated names
      (is (>= (cmd/count-jobs db :enqueued-jobs opts) 0))
      (is (>= (cmd/count-jobs db :scheduled-jobs opts) 0))
      (is (>= (cmd/count-jobs db :dead-jobs opts) 0)))))

(deftest upsert-sql-generation-test
  (testing "Database-specific upsert SQL generation"
    (let [sqlite-upsert (jdbc-utils/upsert-sql :sqlite "test_table" ["id" "name"] ["id"] ["name"])
          postgresql-upsert (jdbc-utils/upsert-sql :postgresql "test_table" ["id" "name"] ["id"] ["name"])
          mysql-upsert (jdbc-utils/upsert-sql :mysql "test_table" ["id" "name"] ["id"] ["name"])]

      (is (re-find #"INSERT OR REPLACE" sqlite-upsert))
      (is (re-find #"ON CONFLICT.*DO UPDATE" postgresql-upsert))
      (is (re-find #"ON DUPLICATE KEY UPDATE" mysql-upsert)))))

(deftest batch-insert-sql-generation-test
  (testing "Batch insert SQL generation"
    (let [sqlite-batch (jdbc-utils/batch-insert-sql :sqlite "test_table" ["id" "name"] 3)
          postgresql-batch (jdbc-utils/batch-insert-sql :postgresql "test_table" ["id" "name"] 3)]

      ;; Both should have proper structure
      (is (re-find #"INSERT INTO.*VALUES.*\\?,.*\\?" sqlite-batch))
      (is (re-find #"INSERT INTO.*VALUES.*\\?,.*\\?" postgresql-batch))

      ;; PostgreSQL should have quoted identifiers
      (is (re-find #"\"test_table\"" postgresql-batch))
      (is (re-find #"\"id\"" postgresql-batch))

      ;; SQLite should not have quoted identifiers
      (is (not (re-find #"\"test_table\"" sqlite-batch)))
      (is (re-find #"test_table" sqlite-batch)))))

(deftest cross-database-sql-generation-test
  (testing "SQL generation produces valid database-specific syntax"
    (doseq [db-type [:sqlite :postgresql :mysql]]
      (testing (str "SQL generation for " db-type)
        (let [dequeue-sql (jdbc-utils/dequeue-sql db-type "enqueued_jobs" "test-queue")]

          ;; all databases should have basic sql
          (is (contains? dequeue-sql :select-sql))
          (is (contains? dequeue-sql :delete-sql))

          ;; postgresql and mysql should have skip locked
          (when (#{:postgresql :mysql} db-type)
            (is (re-find #"SKIP LOCKED" (:select-sql dequeue-sql))))

          ;; sqlite should not have skip locked
          (when (= :sqlite db-type)
            (is (not (re-find #"SKIP LOCKED" (:select-sql dequeue-sql))))))))))
