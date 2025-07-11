-- Goose JDBC Broker SQLite Schema
-- This schema must be installed manually by the user before using the JDBC broker.
-- 
-- Installation:
-- 1. Create SQLite database file
-- 2. Run this script with sqlite3 command
-- 3. Configure your JDBC broker with appropriate table names if you modify them
--
-- Example installation:
-- sqlite3 your_database.db < tables.sql

-- Main job queue table
CREATE TABLE IF NOT EXISTS enqueued_jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BLOB NOT NULL,
    retry_opts BLOB,
    enqueued_at INTEGER NOT NULL,
    priority INTEGER DEFAULT 0,
    batch_id TEXT,
    created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BLOB NOT NULL,
    retry_opts BLOB,
    enqueued_at INTEGER NOT NULL,
    scheduled_at INTEGER NOT NULL,
    batch_id TEXT,
    created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Cron jobs table
CREATE TABLE IF NOT EXISTS cron_jobs (
    name TEXT PRIMARY KEY,
    cron_expression TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BLOB NOT NULL,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    retry_opts BLOB,
    last_scheduled_at INTEGER,
    created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Dead jobs table
CREATE TABLE IF NOT EXISTS dead_jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BLOB NOT NULL,
    retry_opts BLOB,
    enqueued_at INTEGER NOT NULL,
    died_at INTEGER NOT NULL,
    exception_type TEXT,
    exception_message TEXT,
    exception_trace BLOB,
    batch_id TEXT,
    created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Batches table
CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    callback_fn_sym TEXT NOT NULL,
    linger_sec INTEGER NOT NULL,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    retry_opts BLOB,
    total INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'in-progress',
    created_at INTEGER NOT NULL,
    completed_at INTEGER
);

-- Batch jobs tracking table
CREATE TABLE IF NOT EXISTS batch_jobs (
    job_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'enqueued',
    created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
    completed_at INTEGER
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_queue ON enqueued_jobs (queue);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_ready_queue ON enqueued_jobs (ready_queue);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_priority ON enqueued_jobs (priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_batch_id ON enqueued_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_scheduled_at ON scheduled_jobs (scheduled_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_batch_id ON scheduled_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_dead_jobs_died_at ON dead_jobs (died_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_jobs_batch_id ON dead_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_batch_jobs_batch_id ON batch_jobs (batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs (status);