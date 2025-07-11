-- Goose JDBC Broker PostgreSQL Schema
-- This schema must be installed manually by the user before using the JDBC broker.
-- 
-- Installation:
-- 1. Create database and user with appropriate permissions
-- 2. Run this script as a database migration or directly with psql
-- 3. Configure your JDBC broker with appropriate table names if you modify them
--
-- Example installation:
-- psql -h localhost -U postgres -d your_database -f tables.sql

CREATE SCHEMA IF NOT EXISTS goose;

-- Main job queue table
CREATE TABLE IF NOT EXISTS goose.enqueued_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BYTEA NOT NULL,
    retry_opts BYTEA,
    enqueued_at BIGINT NOT NULL,
    priority INTEGER DEFAULT 0,
    batch_id TEXT,
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
);

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS goose.scheduled_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BYTEA NOT NULL,
    retry_opts BYTEA,
    enqueued_at BIGINT NOT NULL,
    scheduled_at BIGINT NOT NULL,
    batch_id TEXT,
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
);

-- Cron jobs table
CREATE TABLE IF NOT EXISTS goose.cron_jobs (
    name TEXT PRIMARY KEY,
    cron_expression TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BYTEA NOT NULL,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    retry_opts BYTEA,
    last_scheduled_at BIGINT,
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
);

-- Dead jobs table
CREATE TABLE IF NOT EXISTS goose.dead_jobs (
    id VARCHAR(255) PRIMARY KEY,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    execute_fn_sym TEXT NOT NULL,
    args BYTEA NOT NULL,
    retry_opts BYTEA,
    enqueued_at BIGINT NOT NULL,
    died_at BIGINT NOT NULL,
    exception_type TEXT,
    exception_message TEXT,
    exception_trace BYTEA,
    batch_id TEXT,
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
);

-- Batches table
CREATE TABLE IF NOT EXISTS goose.batches (
    id VARCHAR(255) PRIMARY KEY,
    callback_fn_sym TEXT NOT NULL,
    linger_sec INTEGER NOT NULL,
    queue TEXT NOT NULL,
    ready_queue TEXT NOT NULL,
    retry_opts BYTEA,
    total INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'in-progress',
    created_at BIGINT NOT NULL,
    completed_at BIGINT
);

-- Batch jobs tracking table
CREATE TABLE IF NOT EXISTS goose.batch_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'enqueued',
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
    completed_at BIGINT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_queue ON goose.enqueued_jobs (queue);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_ready_queue ON goose.enqueued_jobs (ready_queue);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_priority ON goose.enqueued_jobs (priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_enqueued_jobs_batch_id ON goose.enqueued_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_scheduled_at ON goose.scheduled_jobs (scheduled_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_batch_id ON goose.scheduled_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_dead_jobs_died_at ON goose.dead_jobs (died_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_jobs_batch_id ON goose.dead_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_batch_jobs_batch_id ON goose.batch_jobs (batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON goose.batch_jobs (status);