ADR: JDBC SQLite Concurrent Worker Safety
=========================================

Rationale
---------

SQLite, unlike PostgreSQL and MySQL, does not support the `FOR UPDATE SKIP
LOCKED` clause that the JDBC broker relies on for safe concurrent job dequeuing.
Without this feature, multiple workers can select the same job simultaneously,
leading to duplicate job execution.

We need to avoid race conditions like:
1. Worker A begins transaction and SELECTs job X
2. Worker B begins transaction and SELECTs the same job X
3. Worker A DELETEs job X and commits
4. Worker B attempts to DELETE job X (already gone) but still processes it

This violates Goose's guarantee that each job is processed exactly once.

To support SQLite safely, we needed a different approach that provides mutual
exclusion without relying on row-level locking features.

Decision
--------

Implemented a claim-based dequeuing mechanism specifically for SQLite that uses
atomic UPDATE operations to ensure only one worker can claim a job.

The approach:
1. Add `claimed_by` and `claimed_at` columns to job tables
2. Workers claim jobs via UPDATE with their unique worker ID
3. Workers only process jobs they successfully claimed
4. Orphaned claims are cleaned up after a timeout

This requires:
- Worker IDs to identify which worker claimed which job
- Schema changes to add claiming columns
- Different SQL strategies based on database type
- Cleanup mechanism for crashed workers

Avoided Designs
---------------

### Single Shared Connection

Force all SQLite workers to share a single connection, serializing all access.

**Pros:**
- Guarantees no race conditions
- No schema changes needed
- Simple to implement

**Cons:**
- Eliminates concurrency entirely (including running multiple processes)
- Single point of failure
- Poor performance scaling

### External Locking Service

Use Redis or similar for distributed locking of jobs.

**Pros:**
- Works across multiple processes
- Proven distributed locking patterns
- No SQLite-specific code needed

**Cons:**
- Adds an external dependency and complexity.. if you have Redis, why not use the redis broker?

Trade-offs
----------

The claim-based approach adds complexity but provides correct concurrent
behavior. SQLite users accept performance limitations but expect correctness.
The implementation maintains the same broker API while handling SQLite's
limitations transparently.

This design acknowledges that SQLite's write serialization limits throughput but
ensures jobs are never processed multiple times.

For high-throughput scenarios,users should obviously use PostgreSQL or MySQL.
