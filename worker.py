# Background worker — runs as asyncio tasks started on FastAPI lifespan.
# Two loops:
#   1. Event processor: pulls unprocessed events, matches rules, sends DMs
#   2. DM sender: picks up pending DMs and sends them to the API
#   3. Status poller: checks delivery status for queued DMs

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from models import DeletedComment, DmSend, DuplicateBlock, Event, Rule

logger = logging.getLogger("linkplease.worker")

PSEUDOGRAM_BASE_URL = "https://pseudogram-api.onrender.com"
MAX_RETRY_COUNT = 5  # After 5 failed attempts, mark as failed
BACKOFF_BASE_SECONDS = 1  # Exponential backoff: 1, 2, 4, 8, 16 seconds

# Rate limiting state — tracks when we can next send after a 429
_rate_limit_until: datetime | None = None


def _get_api_key() -> str:
    key = os.environ.get("PSEUDOGRAM_API_KEY", "")
    if not key:
        logger.error("PSEUDOGRAM_API_KEY not set!")
    return key


# ===========================================================================
# Loop 1: Event processor
# ===========================================================================

async def process_events(session_factory, http_client: httpx.AsyncClient):
    """Main event processing loop. Runs continuously, polling for unprocessed events."""
    while True:
        try:
            await _process_event_batch(session_factory)
        except Exception:
            logger.exception("Error in event processing loop")
        await asyncio.sleep(1)


async def _process_event_batch(session_factory):
    """Process a batch of unprocessed events.

    Key design: each event is processed in its own session/transaction so that
    an IntegrityError (from dedup) doesn't poison the session for other events.
    """
    # First, fetch the list of unprocessed event IDs
    with session_factory() as session:
        rows = (
            session.execute(
                select(Event.id, Event.event_id, Event.event_type, Event.raw_payload)
                .where(Event.processed == 0)
                .order_by(Event.received_at.asc())
                .limit(50)
            )
            .all()
        )

    if not rows:
        return

    # Fetch all rules once per batch
    with session_factory() as session:
        rules = session.execute(select(Rule)).scalars().all()
        # Detach by extracting data we need
        rule_data = [
            {"rule_id": r.rule_id, "keyword": r.keyword, "dm_message": r.dm_message}
            for r in rules
        ]

    for row in rows:
        evt_db_id, event_id, event_type, raw_payload = row
        try:
            payload = json.loads(raw_payload)

            if event_type == "comment.deleted":
                _handle_comment_deleted(session_factory, event_id, payload)
            elif event_type == "comment.created":
                _handle_comment_created(session_factory, event_id, payload, rule_data)
            else:
                logger.warning(f"Unknown event type: {event_type}")

            # Mark event as processed — separate transaction
            with session_factory() as session:
                session.execute(
                    update(Event).where(Event.id == evt_db_id).values(processed=1)
                )
                session.commit()

        except Exception:
            logger.exception(f"Error processing event {event_id}")


def _handle_comment_deleted(session_factory, event_id: str, payload: dict):
    """Handle comment.deleted events. Record the comment_id as deleted so we
    never send a DM for it. If a DM was already sent, no harm — we just track it."""
    comment_id = payload.get("data", {}).get("comment_id")
    if not comment_id:
        logger.warning(f"comment.deleted event {event_id} missing comment_id")
        return

    with session_factory() as session:
        try:
            deleted = DeletedComment(comment_id=comment_id)
            session.add(deleted)
            session.commit()
            logger.info(f"Recorded deleted comment: {comment_id}")
        except IntegrityError:
            session.rollback()
            logger.debug(f"Comment {comment_id} already marked as deleted")


def _handle_comment_created(
    session_factory, event_id: str, payload: dict, rule_data: list[dict]
):
    """Handle comment.created events: match against rules, create DM send rows.

    Each rule match gets its own session so that a dedup IntegrityError on one
    rule doesn't affect processing of other rules for the same event.
    """
    data = payload.get("data", {})
    comment_text = data.get("text", "")
    comment_id = data.get("comment_id", "")
    user_id = data.get("from", {}).get("user_id", "")

    if not comment_text or not user_id or not comment_id:
        logger.warning(f"Event {event_id} has missing data fields")
        return

    # Check if this comment has been deleted
    with session_factory() as session:
        deleted = session.execute(
            select(DeletedComment).where(DeletedComment.comment_id == comment_id)
        ).scalar_one_or_none()

    if deleted:
        logger.info(f"Skipping DM for deleted comment {comment_id} (event {event_id})")
        return

    # Match comment text against all rules (case-insensitive substring)
    comment_lower = comment_text.lower()
    for rule in rule_data:
        if rule["keyword"].lower() in comment_lower:
            idempotency_key = f"{rule['rule_id']}:{user_id}"

            # Try to create a dm_sends row — the UNIQUE(user_id, rule_id) constraint
            # is our dedup mechanism.
            with session_factory() as session:
                try:
                    dm_send = DmSend(
                        user_id=user_id,
                        rule_id=rule["rule_id"],
                        comment_id=comment_id,
                        idempotency_key=idempotency_key,
                        status="pending",
                    )
                    session.add(dm_send)
                    session.commit()
                    logger.info(
                        f"Created DM send: user={user_id}, rule={rule['rule_id']}, comment={comment_id}"
                    )
                except IntegrityError:
                    session.rollback()
                    # Record the duplicate block — separate transaction to guarantee it persists
                    with session_factory() as dup_session:
                        dup = DuplicateBlock(
                            user_id=user_id,
                            rule_id=rule["rule_id"],
                            event_id=event_id,
                        )
                        dup_session.add(dup)
                        dup_session.commit()
                    logger.info(
                        f"Duplicate blocked: user={user_id}, rule={rule['rule_id']}"
                    )


# ===========================================================================
# Loop 2: DM sender
# ===========================================================================

async def send_pending_dms(session_factory, http_client: httpx.AsyncClient):
    """Loop that picks up pending DM sends and fires them to the API."""
    while True:
        try:
            await _send_dm_batch(session_factory, http_client)
        except Exception:
            logger.exception("Error in DM send loop")
        await asyncio.sleep(1)


async def _send_dm_batch(session_factory, http_client: httpx.AsyncClient):
    """Send a batch of pending DMs to the Pseudogram API."""
    global _rate_limit_until

    # Respect rate limiting — if we've been told to wait, wait
    if _rate_limit_until and datetime.now(timezone.utc) < _rate_limit_until:
        return

    now = datetime.now(timezone.utc)

    with session_factory() as session:
        # Fetch pending DMs that are ready to send (respecting backoff schedule)
        pending = (
            session.execute(
                select(DmSend)
                .where(
                    DmSend.status == "pending",
                    # Only pick up sends whose next_retry_at has passed (or is NULL = first attempt)
                    (DmSend.next_retry_at == None) | (DmSend.next_retry_at <= now),  # noqa: E711
                )
                .order_by(DmSend.created_at.asc())
                .limit(5)  # Small batches to stay under rate limit
            )
            .scalars()
            .all()
        )

        if not pending:
            return

        api_key = _get_api_key()

        for dm_send in pending:
            # Check if this comment was deleted while we were waiting
            deleted = session.execute(
                select(DeletedComment).where(
                    DeletedComment.comment_id == dm_send.comment_id
                )
            ).scalar_one_or_none()

            if deleted:
                dm_send.status = "failed"
                dm_send.updated_at = datetime.now(timezone.utc)
                session.commit()
                logger.info(
                    f"Skipped DM {dm_send.id}: comment {dm_send.comment_id} was deleted"
                )
                continue

            # Respect rate limiting between sends within a batch
            if _rate_limit_until and datetime.now(timezone.utc) < _rate_limit_until:
                logger.info("Rate limit active, stopping batch")
                break

            # Look up the DM message for this rule
            rule = session.execute(
                select(Rule).where(Rule.rule_id == dm_send.rule_id)
            ).scalar_one_or_none()

            if not rule:
                logger.error(f"Rule {dm_send.rule_id} not found for DM send {dm_send.id}")
                dm_send.status = "failed"
                dm_send.updated_at = datetime.now(timezone.utc)
                session.commit()
                continue

            try:
                response = await http_client.post(
                    f"{PSEUDOGRAM_BASE_URL}/v1/dm/send",
                    json={
                        "recipient_user_id": dm_send.user_id,
                        "message": rule.dm_message,
                        "comment_id": dm_send.comment_id,
                    },
                    headers={
                        "X-API-Key": api_key,
                        "Idempotency-Key": dm_send.idempotency_key,
                    },
                    timeout=10.0,
                )

                if response.status_code == 202:
                    # Success — DM queued by the API
                    resp_data = response.json()
                    dm_send.dm_id = resp_data.get("dm_id")
                    dm_send.status = "queued"
                    dm_send.updated_at = datetime.now(timezone.utc)
                    session.commit()
                    logger.info(f"DM queued: dm_id={dm_send.dm_id}")

                elif response.status_code == 429:
                    # Rate limited — back off per Retry-After header
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    _rate_limit_until = datetime.now(timezone.utc) + timedelta(
                        seconds=retry_after
                    )
                    logger.warning(f"Rate limited, backing off {retry_after}s")
                    break  # Stop processing this batch

                elif response.status_code in (400, 401):
                    # 400 = malformed payload, 401 = bad API key — never retry either
                    dm_send.status = "failed"
                    dm_send.updated_at = datetime.now(timezone.utc)
                    session.commit()
                    logger.error(
                        f"DM send {dm_send.id} got {response.status_code}, marking failed: "
                        f"{response.text}"
                    )

                elif response.status_code >= 500:
                    # Transient server error — retry with exponential backoff
                    dm_send.retry_count += 1
                    if dm_send.retry_count >= MAX_RETRY_COUNT:
                        dm_send.status = "failed"
                        logger.error(
                            f"DM send {dm_send.id} exhausted retries after {MAX_RETRY_COUNT} attempts"
                        )
                    else:
                        backoff = BACKOFF_BASE_SECONDS * (2 ** (dm_send.retry_count - 1))
                        dm_send.next_retry_at = datetime.now(timezone.utc) + timedelta(
                            seconds=backoff
                        )
                        logger.warning(
                            f"DM send {dm_send.id} got {response.status_code}, retry #{dm_send.retry_count} "
                            f"in {backoff}s"
                        )
                    dm_send.updated_at = datetime.now(timezone.utc)
                    session.commit()

                else:
                    # Any other unexpected status — treat as non-retryable
                    dm_send.status = "failed"
                    dm_send.updated_at = datetime.now(timezone.utc)
                    session.commit()
                    logger.error(
                        f"Unexpected status {response.status_code} for DM send {dm_send.id}: "
                        f"{response.text}"
                    )

            except httpx.TimeoutException:
                logger.warning(f"Timeout sending DM {dm_send.id}, will retry")
                dm_send.retry_count += 1
                if dm_send.retry_count >= MAX_RETRY_COUNT:
                    dm_send.status = "failed"
                else:
                    backoff = BACKOFF_BASE_SECONDS * (2 ** (dm_send.retry_count - 1))
                    dm_send.next_retry_at = datetime.now(timezone.utc) + timedelta(
                        seconds=backoff
                    )
                dm_send.updated_at = datetime.now(timezone.utc)
                session.commit()

            except Exception:
                logger.exception(f"Unexpected error sending DM {dm_send.id}")
                session.rollback()


# ===========================================================================
# Loop 3: Status poller
# ===========================================================================

async def poll_dm_status(session_factory, http_client: httpx.AsyncClient):
    """Loop that polls delivery status for queued DMs until they reach terminal state."""
    while True:
        try:
            await _poll_status_batch(session_factory, http_client)
        except Exception:
            logger.exception("Error in status polling loop")
        await asyncio.sleep(2)  # Poll every 2 seconds


async def _poll_status_batch(session_factory, http_client: httpx.AsyncClient):
    """Check delivery status for all queued DMs."""
    api_key = _get_api_key()

    with session_factory() as session:
        queued = (
            session.execute(
                select(DmSend).where(
                    DmSend.status == "queued",
                    DmSend.dm_id != None,  # noqa: E711
                )
            )
            .scalars()
            .all()
        )

        if not queued:
            return

        for dm_send in queued:
            try:
                response = await http_client.get(
                    f"{PSEUDOGRAM_BASE_URL}/v1/dm/{dm_send.dm_id}",
                    headers={"X-API-Key": api_key},
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status", "queued")

                    if status in ("delivered", "failed"):
                        dm_send.status = status
                        dm_send.updated_at = datetime.now(timezone.utc)
                        session.commit()
                        logger.info(f"DM {dm_send.dm_id} resolved to {status}")
                    # If still "queued", we'll poll again next cycle

                else:
                    logger.warning(
                        f"Status poll for {dm_send.dm_id} returned {response.status_code}"
                    )

            except Exception:
                logger.exception(f"Error polling status for {dm_send.dm_id}")
