# LinkPlease

Automated Instagram DM engine for the Pseudogram mock API. Built as a reliability engineering challenge — designed to handle duplicates, rate limits, transient failures, and out-of-order events gracefully.

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Set your API key
cp .env.example .env
# Edit .env and set PSEUDOGRAM_API_KEY=<your_key>

# Run locally
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Architecture

```
POST /webhook  →  [events table]  →  background worker  →  POST /v1/dm/send
                                                          →  GET /v1/dm/{id} (poll)
POST /rules    →  [rules table]
GET  /stats    →  live SQL counts from [dm_sends] + [duplicate_blocks]
```

### Key design decisions

1. **DB-level dedup** — `UNIQUE(event_id)` prevents duplicate events; `UNIQUE(user_id, rule_id)` prevents duplicate DM sends. The `IntegrityError` exception IS the dedup check, not an in-memory set that could race.

2. **Idempotency-Key on every send** — `{rule_id}:{user_id}` ensures the mock API itself won't create duplicate DMs even if our retry logic double-fires.

3. **Zero synchronous work in `/webhook`** — raw body capture → HMAC verify → DB insert → return 200. All matching and sending happens in async background tasks.

4. **Status polling** — a `202 Accepted` from `/v1/dm/send` does NOT mean delivered. A separate loop polls `GET /v1/dm/{dm_id}` until `delivered` or `failed`, and only then does `/stats` count it as `sent`.

5. **All state in SQLite** — nothing lives only in memory. Process can restart and resume from exactly where it left off.

## API Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/webhook` | POST | Ingest comment events from Pseudogram |
| `/rules` | POST | Create keyword → DM message rules |
| `/stats` | GET | Live stats (sent, failed, queued, duplicates_blocked) |
| `/` | GET | Health check |

## Deployment (Render)

- **Build command**: `pip install -r requirements.txt`
- **Start command**: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- **Environment variable**: `PSEUDOGRAM_API_KEY` (set in Render dashboard, never in code)
