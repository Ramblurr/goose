-- Goose JDBC Broker MySQL Schema
-- This schema must be installed manually by the user before using the JDBC broker.
-- 
-- Installation:
-- 1. Create database first using database.sql
-- 2. Run this script as a database migration or directly with mysql client
-- 3. Configure your JDBC broker with appropriate table names if you modify them
--
-- Example installation:
-- mysql -h localhost -u root -p < database.sql
-- mysql -h localhost -u root -p goose < tables.sql

-- Main job queue table
CREATE TABLE IF NOT EXISTS enqueued_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue VARCHAR(255) NOT NULL,
    ready_queue VARCHAR(255) NOT NULL,
    execute_fn_sym VARCHAR(255) NOT NULL,
    args LONGBLOB NOT NULL,
    retry_opts LONGBLOB,
    enqueued_at BIGINT NOT NULL,
    priority INTEGER DEFAULT 0,
    batch_id VARCHAR(255),
    created_at BIGINT DEFAULT NULL
);

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue VARCHAR(255) NOT NULL,
    ready_queue VARCHAR(255) NOT NULL,
    execute_fn_sym VARCHAR(255) NOT NULL,
    args LONGBLOB NOT NULL,
    retry_opts LONGBLOB,
    enqueued_at BIGINT NOT NULL,
    scheduled_at BIGINT NOT NULL,
    batch_id VARCHAR(255),
    created_at BIGINT DEFAULT NULL
);

-- Cron jobs table
CREATE TABLE IF NOT EXISTS cron_jobs (
    name VARCHAR(255) PRIMARY KEY,
    cron_expression VARCHAR(255) NOT NULL,
    execute_fn_sym VARCHAR(255) NOT NULL,
    args LONGBLOB NOT NULL,
    queue VARCHAR(255) NOT NULL,
    ready_queue VARCHAR(255) NOT NULL,
    retry_opts LONGBLOB,
    last_scheduled_at BIGINT,
    created_at BIGINT DEFAULT NULL
);

-- Dead jobs table
CREATE TABLE IF NOT EXISTS dead_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue VARCHAR(255) NOT NULL,
    ready_queue VARCHAR(255) NOT NULL,
    execute_fn_sym VARCHAR(255) NOT NULL,
    args LONGBLOB NOT NULL,
    retry_opts LONGBLOB,
    enqueued_at BIGINT NOT NULL,
    died_at BIGINT NOT NULL,
    exception_type VARCHAR(255),
    exception_message TEXT,
    exception_trace LONGBLOB,
    batch_id VARCHAR(255),
    created_at BIGINT DEFAULT NULL
);

-- Batches table
CREATE TABLE IF NOT EXISTS batches (
    id VARCHAR(255) PRIMARY KEY,
    callback_fn_sym VARCHAR(255) NOT NULL,
    linger_sec INTEGER NOT NULL,
    queue VARCHAR(255) NOT NULL,
    ready_queue VARCHAR(255) NOT NULL,
    retry_opts LONGBLOB,
    total INTEGER NOT NULL,
    status VARCHAR(255) NOT NULL DEFAULT 'in-progress',
    created_at BIGINT NOT NULL,
    completed_at BIGINT
);

-- Batch jobs tracking table
CREATE TABLE IF NOT EXISTS batch_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    batch_id VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL DEFAULT 'enqueued',
    created_at BIGINT DEFAULT NULL,
    completed_at BIGINT
);

-- Indexes for performance
CREATE INDEX idx_enqueued_jobs_queue ON enqueued_jobs (queue);
CREATE INDEX idx_enqueued_jobs_ready_queue ON enqueued_jobs (ready_queue);
CREATE INDEX idx_enqueued_jobs_priority ON enqueued_jobs (priority DESC, created_at ASC);
CREATE INDEX idx_enqueued_jobs_batch_id ON enqueued_jobs (batch_id);

CREATE INDEX idx_scheduled_jobs_scheduled_at ON scheduled_jobs (scheduled_at);
CREATE INDEX idx_scheduled_jobs_batch_id ON scheduled_jobs (batch_id);

CREATE INDEX idx_dead_jobs_died_at ON dead_jobs (died_at DESC);
CREATE INDEX idx_dead_jobs_batch_id ON dead_jobs (batch_id);

CREATE INDEX idx_batch_jobs_batch_id ON batch_jobs (batch_id);
CREATE INDEX idx_batch_jobs_status ON batch_jobs (status);