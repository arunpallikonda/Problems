1. Fix the <1,000 Kafka batch bug
Why: The Kafka Event Processor only commits offsets and continues polling if a batch returns ≥1,000 messages. If a poll returns fewer (e.g. 2 messages during a quiet period), those messages may never get processed or get indefinitely delayed.
How to solve: Review the Kafka polling loop logic. The commit-and-repoll behavior should not be gated on batch size. Instead, always commit and continue polling regardless of how many records were returned. Only stop polling when the poll returns 0 records. Add a test case simulating a small batch (2–5 messages) to verify they get processed end to end.

2. Run Kafka and DB Reader threads in parallel safely
Why: Currently the DB Reader thread waits for the Kafka thread to fully drain all topics before starting. This means during a 2M–6M message date roll, nothing gets processed in DB until all messages are ingested — adding significant end-to-end latency.
How to solve: Add a received_at timestamp column to master_file_events if not already present. Modify the DB Reader to run continuously and independently, but only pick up events where received_at < NOW() - 1 minute. This creates a safe settled window so the DB Reader isn't racing with Kafka inserts on the same rows or conflicting with the ORDER BY event_id query.

3. Verify and optimize the queue mechanism
Why: A recent change introduced a sub-thread that reads from master_file_events and pushes into an in-memory queue, which the DB Reader then consumes in batches of ~30K. This is the right direction but needs to be verified it's working correctly and not introducing its own bottlenecks.
How to solve: Trace the full flow — confirm the sub-thread is running, the queue is being populated, and the DB Reader is consuming from it correctly. Check for queue overflow scenarios during peak load (6M messages). Add metrics or logging around queue depth, throughput, and lag so you can observe behavior during a date roll simulation.

4. Scope deduplication strictly by topic
Why: message_id (RTC ID) is unique within a topic but not across topics — the same ID value can appear in both a derivative message and a product message referring to completely different entities. If deduplication is applied globally across topics, you risk incorrectly marking valid events as duplicates.
How to solve: Ensure all deduplication queries on master_file_events include topic_name as part of the grouping or WHERE clause alongside message_id. Review every query that touches message_id and confirm none of them operate across topics without that filter.

5. Optimize SELECT-based change detection queries
Why: For every Kafka message, the app runs at least 2 SELECT queries against the DB2 target tables — one to check if the row exists by primary key, and one to check if any values have changed. At 2M–6M messages post date roll, this is potentially 4M–12M SELECT queries, which is the core DB performance bottleneck. MERGE queries cannot be used due to legacy triggers.
How to solve: First, run EXPLAIN plans on the change detection SELECT queries for the highest volume tables (especially option_series) to identify full table scans or inefficient access paths. Add indexes where missing. Second, explore batching the change detection — instead of one SELECT per message, build a batch SELECT that checks multiple rows at once and returns only the ones that differ. Third, consider maintaining a lightweight in-memory or Redis cache of recently seen entity states to avoid hitting DB2 at all for unchanged data during date roll republishing.

6. Document legacy triggers on DB2 tables
Why: Before/after insert triggers on target tables are blocking the use of MERGE queries and constraining optimization options. Currently nobody has a clear map of which tables have triggers, what they do, and what would break if anything changed. This is a risk and a blocker for future optimization work.
How to solve: Query the DB2 system catalog to extract all triggers across the relevant tables (option_series, valuable_item, and any other tables written to by RDS Adapter). Document trigger name, table, timing (before/after), event (insert/update/delete), and inferred purpose. This doesn't require changing anything — just understanding what's there so you can design safely around it.

7. Deduplicate at RDS Adapter level during date roll
Why: When RTC fires the same date roll multiple times, RDS republishes the full universe (~2M messages) each time even if nothing changed. RDS Adapter currently processes all of these, resulting in 4M–6M messages being ingested and evaluated — the vast majority being redundant work.
How to solve: After the DB Reader determines an event requires no update (change detection SELECT returns a match), track that message_id + checksum or message_id + received_date in a short-lived in-memory cache. If the same message comes in again within the same business date, skip it before it even hits the change detection query. This reduces DB load dramatically during repeated date rolls.

8. Design multi-instance horizontal scaling support
Why: Currently a single instance runs with 24GB memory. To scale throughput, multiple instances need to run simultaneously. The Kafka side is straightforward (same consumer group = partition distribution). The DB side is the problem — multiple instances reading from master_file_events will pick up the same events and double-process them.
How to solve: Add a claimed_by column (or equivalent) to master_file_events. When the DB Reader sub-thread picks up a batch of events to enqueue, it does an atomic UPDATE to set claimed_by = <instance_id> and status = CLAIMED before processing. Only rows where claimed_by IS NULL and status = RECEIVED are eligible for pickup. This ensures each event is processed by exactly one instance. Use DB2 row-level locking or optimistic locking on this update to handle concurrent claims safely.

9. Profile and optimize Kafka ingest speed
Why: Messages arrive as Protobuf and are deserialized to JSON before being written to master_file_events. The target is to ingest 2M messages in 1–2 minutes. Current performance doesn't meet this bar and the bottleneck hasn't been identified yet.
How to solve: Add timing instrumentation around: (a) Protobuf deserialization, (b) JSON conversion, (c) DB insert into master_file_events. Identify which step dominates. If deserialization is slow, evaluate whether you can write raw Protobuf bytes to the table and deserialize later. If DB insert is slow, switch to batch inserts (insert 500–1000 rows per statement instead of one at a time) and check for missing indexes on master_file_events itself.

10. Introduce business key for deduplication and thread dispatch
Why: After corporate actions or adjustments, the same real-world entity (e.g. Apple Equity) can get a new RTC ID. Currently dedup and thread dispatch are based on message_id (RTC ID), so two different RTC IDs for the same entity get dispatched to two different threads simultaneously — both try to write to the same row in valuable_item table, causing a race condition and incorrect data.
How to solve: Add a business_key column to master_file_events — a composite string derived from entity type + natural business identifiers (e.g. INSTRUMENT|AAPL|XNAS). Populate it during Kafka ingestion. Change the deduplication and thread dispatch logic to group by business_key instead of message_id — only dispatch the latest event per business_key to a thread, ensuring no two threads ever touch the same DB row simultaneously.

11. Decide whether to skip or sequentially process duplicate events
Why: Current design marks all but the latest event for a given message_id (or future business_key) as OLD_EVENT_SKIPPED and only processes the last one. This is efficient but may be incorrect if intermediate states matter — for example, if an entity goes through a meaningful state transition that needs to be recorded in the audit trail.
How to solve: Review with the business/data team whether intermediate event states are ever meaningful. If latest-state-only is confirmed correct, keep the current skip logic. If intermediate states matter, change the DB Reader to process all events for a business key sequentially in event_id order, one at a time, before moving to the next business key. Document the decision either way.

12. Investigate and fix option_series table indexing
Why: The instrument topic carries ~80% of all message volume, and the majority of those messages write to the option_series table. SELECT and UPDATE queries on this table are running slow, making it the single biggest DB-side bottleneck by volume.
How to solve: Run DB2 EXPLAIN on the change detection SELECT query and the UPDATE query for option_series. Look for table scans, sort overhead, or index misses. The most impactful indexes are typically on the columns used in the WHERE clause of the change detection SELECT (primary key columns + any columns used in the value comparison). Add a composite index if the current one doesn't cover the full query. After adding indexes, re-run EXPLAIN to confirm the access path improved and measure query time before/after.
