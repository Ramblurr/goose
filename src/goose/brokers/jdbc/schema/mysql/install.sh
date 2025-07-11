#!/bin/bash
# Goose JDBC Broker MySQL Schema Installation Script
#
# This script installs the Goose JDBC broker schema for MySQL.
# You may need to modify the connection parameters for your environment.
#
# Usage:
#   ./install.sh [DATABASE_NAME] [HOST] [PORT] [USER]
#
# Environment variables (if not provided as arguments):
#   MYSQL_DATABASE - database name (default: goose)
#   MYSQL_HOST     - host (default: localhost)
#   MYSQL_PORT     - port (default: 3306)
#   MYSQL_USER     - user (default: root)
#   MYSQL_PASSWORD - password (will prompt if not set)

set -e

DATABASE=${1:-${MYSQL_DATABASE:-goose}}
HOST=${2:-${MYSQL_HOST:-localhost}}
PORT=${3:-${MYSQL_PORT:-3306}}
USER=${4:-${MYSQL_USER:-root}}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing Goose JDBC broker schema for MySQL..."
echo "Database: $DATABASE"
echo "Host: $HOST"
echo "Port: $PORT"
echo "User: $USER"
echo

if [ -n "$MYSQL_PASSWORD" ]; then
    mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$MYSQL_PASSWORD" < "$SCRIPT_DIR/tables.sql"
else
    mysql -h "$HOST" -P "$PORT" -u "$USER" -p < "$SCRIPT_DIR/tables.sql"
fi

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