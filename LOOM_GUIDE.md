# 3-Minute Loom Video Guide & Script

The evaluation requires a 3-minute video answering:
1. **One major trade-off made and what was given up**
2. **What you would do differently with one more week**

Here is a structured, concise talking points guide for your recording:

---

## 🕒 Timing Breakdown (Total: 3:00)

### 0:00 - 0:40 | Quick Overview & Architecture
- **Screen**: Show the codebase (FastAPI `app.py`, `models.py`, `worker.py`) or live `/stats` endpoint.
- **Key points**:
  - LinkPlease is designed around a **two-tier database-level deduplication strategy** and a **persistent state machine** in SQLite with WAL mode.
  - `/webhook` is strictly an ingestion gateway (fast raw-bytes capture, HMAC-SHA256 signature verification, instant return 200).
  - Background workers handle asynchronous matching, outbound DM dispatching with exponential backoff on 500s, `Retry-After` rate-limit compliance, and delivery polling.

### 0:40 - 1:40 | Trade-off Made & What Was Given Up
- **Screen**: Show `models.py` (the `UNIQUE(user_id, rule_id)` constraint) or `worker.py`.
- **Trade-off**: **Using In-Process Asyncio Workers with SQLite State Persistence vs. External Distributed Queue (Celery/Redis/Postgres)**.
- **Why we chose it**:
  - Kept infrastructure completely zero-dependency and deterministic.
  - Every retry, backoff timer, and queued status is backed by database rows (survives container restarts).
- **What was given up**:
  - Write serialization under SQLite means horizontal scale is bounded.
  - Worker tasks run inside the same process lifecycle as the web API, meaning high CPU spikes in background processing could impact event ingestion latency if not carefully managed.

### 1:40 - 2:40 | What I Would Do Differently With One More Week
- **Screen**: Show `FAILURES.md` or the database schema.
- **Three specific improvements**:
  1. **PostgreSQL with `SELECT FOR UPDATE SKIP LOCKED`**:
     - Allows multiple independent background worker containers to pull batches of pending DMs concurrently without lock collisions.
  2. **Part C Delivery Reconciliation Loop**:
     - Implement a sweep job that identifies DMs stuck in `queued` status past a reasonable threshold (e.g. >10 minutes) and polls for resolution or marks them `failed (TIMEOUT)`.
  3. **Adaptive Token Bucket Rate Limiter**:
     - Rather than waiting for a `429` response with `Retry-After`, maintain an internal client-side token bucket (10 req / 60 sec) to proactively smooth outbound traffic spikes.

### 2:40 - 3:00 | Conclusion & Wrap Up
- "Thank you for reviewing. LinkPlease demonstrates that in hostile distributed systems, honesty in status reporting and database-level invariants always beat optimistic in-memory counters."

---

## 🎯 Tips for Recording
- Keep it natural and unscripted.
- Show code / terminal rather than slides.
- Keep within the 3:00 minute limit.
