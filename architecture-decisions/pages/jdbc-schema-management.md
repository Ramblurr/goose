ADR: JDBC Broker Schema Management
==================================

- Users are responsible for installing and managing database schemas
- Goose provides example sql schema files for the supported SQL dbs
- Users have full control over database/schema and table names, but not the column names

Rationale
---------

Touching a relational database schema is tricky business, not to mention projects
have strong opinions about how to manage the migrations. Leaving to the user gives them
maximum flexibility.

Avoided Designs
---------

- Auto-migration system - Adds complexity without clear benefit for job queue use case


User Usage
----------
1. Choose the appropriate SQL file for your database
2. Review and modify table names if needed
3. Install schema using provided SQL files
4. Configure broker with custom table names (if modified)
5. Test in non-production environment
6. Deploy to production

Table names are configurable via broker options:

- `:enqueued-jobs-table` - Main job queue (default: `"enqueued_jobs"`)
- `:scheduled-jobs-table` - Future jobs (default: `"scheduled_jobs"`)  
- `:cron-jobs-table` - Recurring jobs (default: `"cron_jobs"`)
- `:dead-jobs-table` - Failed jobs (default: `"dead_jobs"`)
- `:batches-table` - Batch metadata (default: `"batches"`)
- `:batch-jobs-table` - Batch tracking (default: `"batch_jobs"`)
