ADR: JDBC Connection Pooling Strategy
=====================================

Rationale
---------

The JDBC broker implementation needs to manage database connections efficiently while supporting concurrent worker threads.

Goose should recommend that the user use a connection pooling library, and will accept a `:datasource` (javax.sql.DataSource) as the connection.

Goose should provide clear documentation on pooling requirements:

- Minimum connections = number of workers + 1 (for scheduler)
- Recommended: 2-3x worker count for headroom
- Transaction timeout considerations


Avoided Designs
---------------

### Built-in Connection Pooling

Goose provides its own connection pooling using HikariCP or similar.

**Pros:**
- Optimal performance out of the box
- Can tune pool settings for Goose's specific patterns
- Consistent behavior across deployments

**Cons:**
- Adds dependency on pooling library
- May conflict with user's existing pools
- Configuration complexity

### Per-Worker Thread Connections

Each worker thread maintains its own long-lived connection.

**Pros:**
- No connection acquisition overhead
- No pool contention between workers
- Simple concurrency model

**Cons:**
- Connections held even when idle
- Doesn't handle connection failures well
- Limits worker scalability
