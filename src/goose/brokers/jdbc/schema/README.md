# Goose JDBC Broker Schema Management

The Goose JDBC broker requires database schema to be installed manually before use. This directory contains database-specific SQL files and installation scripts for PostgreSQL, MySQL, and SQLite.

## Why Manual Schema Management?

Starting with this version, Goose requires users to manage their own database schema installation and migrations. This approach provides several benefits:

- **User Control**: You have full control over your database schema, table names, and permissions
- **Environment Flexibility**: Different environments can use different schema names or table prefixes
- **Migration Safety**: Schema changes are explicit and can be reviewed before deployment
- **Production Readiness**: Follows database best practices for production deployments

## Quick Start

### 1. Choose Your Database

Navigate to the appropriate subdirectory:
- `postgresql/` - PostgreSQL 9.5+
- `mysql/` - MySQL 8.0.1+  
- `sqlite/` - SQLite 3+

### 2. Install the Schema

#### Option A: Use the Installation Script (Recommended)
```bash
cd postgresql/
./install.sh [database_name] [host] [port] [user]
```

#### Option B: Run SQL Manually
```bash
# PostgreSQL
psql -h localhost -U postgres -d your_database -f postgresql/tables.sql

# MySQL  
mysql -h localhost -u root -p your_database < mysql/tables.sql

# SQLite
sqlite3 your_database.db < sqlite/tables.sql
```

### 3. Configure Your Broker

Update your Goose configuration to match your table names:

```clojure
(require '[goose.brokers.jdbc.broker :as jdbc])

;; Default table names (matches the SQL files)
(def broker (jdbc/new-producer {:jdbc-url "jdbc:postgresql://localhost/goose"}))

;; Custom table names
(def broker (jdbc/new-producer 
              {:jdbc-url "jdbc:postgresql://localhost/goose"
               :enqueued-jobs-table "myapp.jobs"
               :scheduled-jobs-table "myapp.scheduled_jobs"
               :dead-jobs-table "myapp.dead_jobs"
               :batches-table "myapp.batches"
               :batch-jobs-table "myapp.batch_jobs"
               :cron-jobs-table "myapp.cron_jobs"}))
```

## Table Configuration Options

The JDBC broker accepts the following table name configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `:enqueued-jobs-table` | `"enqueued_jobs"` | Main job queue table |
| `:scheduled-jobs-table` | `"scheduled_jobs"` | Future/delayed jobs |
| `:cron-jobs-table` | `"cron_jobs"` | Recurring job definitions |
| `:dead-jobs-table` | `"dead_jobs"` | Failed jobs |
| `:batches-table` | `"batches"` | Batch job metadata |
| `:batch-jobs-table` | `"batch_jobs"` | Batch job tracking |

## Schema Customization

You can modify the SQL files to:
- Change table names or add prefixes
- Use different schemas/databases  
- Adjust column types or constraints
- Add custom indexes for your workload

**Important**: If you modify table names in the SQL files, make sure to update your broker configuration accordingly.

## Database-Specific Notes

### PostgreSQL
- Uses `BYTEA` for binary data storage
- Uses `FOR UPDATE SKIP LOCKED` for high-concurrency safety
- Default schema is `goose`
- Supports `IF NOT EXISTS` for safe re-runs

### MySQL  
- Uses `LONGBLOB` for binary data storage
- Requires MySQL 8.0.1+ for `SKIP LOCKED` support
- Uses database `goose` by default
- Index creation syntax varies slightly

### SQLite
- Uses `BLOB` for binary data storage  
- Single-process safety through transaction isolation
- File-based storage (specify path in `:database-name`)
- Supports in-memory databases for testing

## Migration Strategy

For existing installations using auto-schema creation:

1. **Backup your data** before proceeding
2. Export your existing schema: `pg_dump --schema-only` or equivalent
3. Compare with the new SQL files to identify any differences
4. Install the new schema in a test environment
5. Update your application configuration
6. Test thoroughly before production deployment

## Troubleshooting

### Permission Errors
Ensure your database user has appropriate permissions:
- `CREATE` - for creating tables and indexes
- `INSERT`, `SELECT`, `UPDATE`, `DELETE` - for job operations
- `CREATE INDEX` - if using custom indexes

### Table Already Exists
The SQL files use `IF NOT EXISTS` where supported. For existing installations, you may need to handle conflicts manually.

### Connection Issues  
Verify your JDBC URL and credentials are correct. Test with a simple connection before running schema installation.

## Examples

See the `examples/` directory in the main Goose repository for complete working examples with different database configurations.