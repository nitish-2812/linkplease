# LinkPlease — Reliability Engineering & Failure Modes Analysis

This document provides a candid, technical analysis of failure modes, architectural trade-offs, edge cases, and known limitations in LinkPlease.

---

## 1. System Architecture & Deduplication Strategy

### Two-Tier Deduplication

1. **Ingestion Level (`events` table)**:
   - `event_id` is protected by a `UNIQUE` database constraint.
   - Any duplicate delivery of the same webhook payload by the mock API triggers an `IntegrityError` upon insertion, which is caught and gracefully no-op'd (returning HTTP 200 within milliseconds without enqueueing redundant work).

2. **Business / Rule Level (`dm_sends` table)**:
   - A composite `UNIQUE(user_id, rule_id)` constraint ensures that a user is contacted at most once per rule trigger.
   - When multiple comments from the same user match the same rule (or when duplicate/out-of-order events arrive), the database constraint violation directly increments our `duplicate_blocks` metric and halts further outbound processing.

3. **Outbound Safety Net (`Idempotency-Key` header)**:
   - Every outbound request to `POST /v1/dm/send` includes `Idempotency-Key: f"{rule_id}:{user_id}"`.
   - Even in edge cases where in-flight network timeouts cause our retry loop to fire a duplicate HTTP request, the mock API server resolves to the existing `dm_id` rather than dispatching a duplicate message.

---

## 2. Documented Failure Modes & Edge Cases

### Failure Mode 1: SQLite File Lock Contention Under Sudden Ingestion Bursts
- **Description**: While SQLite is run with `PRAGMA journal_mode=WAL` and `PRAGMA busy_timeout=5000` to permit concurrent reads alongside writes, SQLite still serializes writes to a single thread/connection.
- **Observed/Potential Impact**: If the ingestion endpoint (`/webhook`) receives high concurrency (e.g. 500 requests in <2 seconds), concurrent write transactions can experience `sqlite3.OperationalError: database is locked` if busy wait thresholds are exceeded.
- **Mitigation Implemented**: 
  - WAL mode and busy timeout configured at the SQLAlchemy connection level.
  - Transactions in `/webhook` and worker loops are kept minimal (sub-millisecond single-row commits).
- **Production Solution**: Migrate SQLite to Postgres or MySQL with connection pooling (e.g. PgBouncer) and row-level locking.

### Failure Mode 2: In-Flight Process Restart & Transient State
- **Description**: If the worker container/process terminates (e.g., Render deployment spin-down or dyno cycling) precisely after `POST /v1/dm/send` returns `202 Accepted` but before the database transaction commits `dm_id` and status `queued`.
- **Observed/Potential Impact**: The DM is queued upstream, but our database still marks it as `pending`. On recovery, our worker will attempt to resend.
- **Mitigation Implemented**: 
  - The deterministic `Idempotency-Key` header (`rule_id:user_id`) ensures the mock API returns the previously generated `dm_id` on the replay, preventing a duplicate message from being sent to the end user.

### Failure Mode 3: Out-of-Order `comment.deleted` Delivery
- **Scenario A (Delete arrives before `comment.created`)**:
  - `comment.deleted` is persisted to `deleted_comments`.
  - When `comment.created` later arrives and is processed, the worker inspects `deleted_comments` by `comment_id`. Finding a record, it cancels DM generation immediately.
- **Scenario B (Delete arrives after `comment.created` but before `send`)**:
  - Worker performs a secondary check on `deleted_comments` right before making the outbound HTTP request. If found, the send status is transitioned to `failed` and skipped.
- **Scenario C (Delete arrives after `202 Accepted` / dispatch)**:
  - If the DM has already been accepted and is sitting in the mock API queue or in transit, it cannot be recalled downstream. This is an inherent distributed systems boundary limitation.

### Failure Mode 4: Mock API Silent Dropping / Incomplete Status Resolution
- **Description**: Outbound DM sends return `202 Accepted`, transitioning the row to `queued`. The poller queries `GET /v1/dm/{dm_id}` every 2 seconds.
- **Observed/Potential Impact**: If the mock API hangs indefinitely in `queued` or encounters an internal leak without resolving to `delivered` or `failed`, the DM row remains in `queued` status indefinitely.
- **Production Solution**: A reconciliation dead-letter loop with a timeout threshold (e.g., after 15 minutes of unresolved `queued` status, transition to `failed` with reason `POLL_TIMEOUT`).

### Failure Mode 5: Rate Limiting (HTTP 429) & Queue Backlog
- **Description**: Mock API enforces a rate limit of 10 requests per rolling 60 seconds.
- **Observed/Potential Impact**: Under a burst of 50 comments matching rules, only 10 will send in the first minute. The remaining 40 remain in `pending` (counted as `queued` in `/stats`).
- **Mitigation Implemented**:
  - Worker parses the `Retry-After` response header and pauses all outbound sends until the deadline expires, avoiding wasted requests and repeated 429s.
  - The state is persisted in the database; no queued messages are dropped during backoff.

---

## 3. Trade-offs Made & Justification

| Decision | What We Gained | What We Gave Up |
|---|---|---|
| **SQLite with WAL instead of Redis/PostgreSQL** | Zero external infrastructure dependencies; single container deployment simplicity; fully transactional state persistence across restarts. | Reduced write concurrency under extreme horizontal scaling (single-writer serialization). |
| **Asyncio In-Process Worker instead of Celery/BullMQ** | Eliminates message broker operational complexity and serialization overhead; keeps application lifecycle unified in FastAPI. | Cannot horizontally scale worker processes across multiple nodes without external coordination. |
| **Substring Matching on Rules** | Satisfies the contract for varied comment text ("PRICE please", "what is the price?") reliably. | Higher false positive rate if a substring matches an unintended word without semantic tokenization. |

---

## 4. What We Would Do With One More Week

1. **Database Migration to Managed PostgreSQL**:
   - Replace SQLite with PostgreSQL with explicit row-level locking (`SELECT FOR UPDATE SKIP LOCKED`) to enable multi-worker horizontal scaling.
2. **Dedicated Queue & Dead Letter Queue (DLQ)**:
   - Introduce RabbitMQ or AWS SQS for message buffering, allowing rate-limited ingestion to buffer cleanly without saturating database connection pools.
3. **Automated Status Reconciliation (Part C)**:
   - Implement scheduled audit jobs comparing local database states with the platform's delivery audit logs to reconcile dropped or zombie `queued` messages.
