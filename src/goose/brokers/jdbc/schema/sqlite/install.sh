#!/bin/bash
# Goose JDBC Broker SQLite Schema Installation Script
#
# This script installs the Goose JDBC broker schema for SQLite.
#
# Usage:
#   ./install.sh [DATABASE_FILE]
#
# Environment variables (if not provided as arguments):
#   SQLITE_DATABASE - database file path (default: goose.db)

set -e

DATABASE_FILE=${1:-${SQLITE_DATABASE:-goose.db}}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing Goose JDBC broker schema for SQLite..."
echo "Database file: $DATABASE_FILE"
echo

sqlite3 "$DATABASE_FILE" < "$SCRIPT_DIR/tables.sql"

echo
echo "Goose JDBC broker schema installed successfully!"
echo "Database file: $DATABASE_FILE"
echo
echo "You can now configure your Goose JDBC broker with these table names:"
echo "  :enqueued-jobs-table    \"enqueued_jobs\""
echo "  :scheduled-jobs-table   \"scheduled_jobs\""
echo "  :cron-jobs-table        \"cron_jobs\""
echo "  :dead-jobs-table        \"dead_jobs\""
echo "  :batches-table          \"batches\""
echo "  :batch-jobs-table       \"batch_jobs\""