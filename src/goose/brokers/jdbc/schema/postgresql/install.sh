#!/bin/bash
# Goose JDBC Broker PostgreSQL Schema Installation Script
#
# This script installs the Goose JDBC broker schema for PostgreSQL.
# You may need to modify the connection parameters for your environment.
#
# Usage:
#   ./install.sh [DATABASE_NAME] [HOST] [PORT] [USER]
#
# Environment variables (if not provided as arguments):
#   PGDATABASE - database name (default: goose)
#   PGHOST     - host (default: localhost)
#   PGPORT     - port (default: 5432)
#   PGUSER     - user (default: postgres)
#   PGPASSWORD - password (will prompt if not set)

set -e

DATABASE=${1:-${PGDATABASE:-goose}}
HOST=${2:-${PGHOST:-localhost}}
PORT=${3:-${PGPORT:-5432}}
USER=${4:-${PGUSER:-postgres}}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing Goose JDBC broker schema for PostgreSQL..."
echo "Database: $DATABASE"
echo "Host: $HOST"
echo "Port: $PORT"
echo "User: $USER"
echo

psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -f "$SCRIPT_DIR/tables.sql"

echo
echo "Goose JDBC broker schema installed successfully!"
echo
echo "You can now configure your Goose JDBC broker with these table names:"
echo "  :enqueued-jobs-table    \"goose.enqueued_jobs\""
echo "  :scheduled-jobs-table   \"goose.scheduled_jobs\""
echo "  :cron-jobs-table        \"goose.cron_jobs\""
echo "  :dead-jobs-table        \"goose.dead_jobs\""
echo "  :batches-table          \"goose.batches\""
echo "  :batch-jobs-table       \"goose.batch_jobs\""