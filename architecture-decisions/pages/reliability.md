ADR: Reliability
=============

Rationale
---------

- Abrupt shutdown of worker won't cause in-flight jobs to be lost
  - Goose uses `HEARTBEATs`, `IN-PROGRESS-JOB queues` & `ORPHAN-CHECKs` to ensure this
- Users can set Redis writetodisk config for more reliable infra

### JDBC Broker
- Leverages database ACID properties for reliable job persistence
- Database transactions ensure job state consistency during failures
- No additional reliability mechanisms needed as database handles durability

Avoided Designs
---------
