# Database models — SQLAlchemy ORM definitions for all three tables.
# Using SQLite with WAL mode for better concurrent read/write behavior.

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


class Event(Base):
    """Raw webhook events. UNIQUE on event_id enforces dedup at the DB level —
    a second INSERT with the same event_id will raise IntegrityError, which we
    catch and treat as a silent no-op (the event was already persisted)."""

    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, unique=True, nullable=False, index=True)
    event_type = Column(String, nullable=False)  # "comment.created" or "comment.deleted"
    raw_payload = Column(Text, nullable=False)  # Original JSON bytes, stored as text
    processed = Column(Integer, default=0, nullable=False)  # 0=unprocessed, 1=processed
    received_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )


class Rule(Base):
    """Keyword → DM message rules. keyword is stored lowercase for
    case-insensitive matching."""

    __tablename__ = "rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(String, unique=True, nullable=False, index=True)
    keyword = Column(String, nullable=False)  # Stored lowercase
    dm_message = Column(Text, nullable=False)
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )


class DmSend(Base):
    """Tracks every DM we intend to send. The UNIQUE constraint on
    (user_id, rule_id) is the primary dedup mechanism — it guarantees
    that we never send the same user the same DM twice, even under
    concurrent processing. We catch the IntegrityError on insert and
    count it as duplicates_blocked.

    Status lifecycle: pending → queued → delivered|failed
    - pending: row created, DM not yet sent to the API
    - queued: API returned 202 Accepted, dm_id is set, awaiting delivery confirmation
    - delivered: poll confirmed delivery (terminal)
    - failed: exhausted retries or poll confirmed failure (terminal)
    """

    __tablename__ = "dm_sends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    rule_id = Column(String, nullable=False)
    comment_id = Column(String, nullable=False)
    dm_id = Column(String, nullable=True)  # Set once API returns 202
    idempotency_key = Column(String, nullable=False)  # f"{rule_id}:{user_id}"
    status = Column(
        String, default="pending", nullable=False
    )  # pending|queued|delivered|failed
    retry_count = Column(Integer, default=0, nullable=False)
    next_retry_at = Column(DateTime, nullable=True)  # For exponential backoff scheduling
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("user_id", "rule_id", name="uq_user_rule"),
    )


class DeletedComment(Base):
    """Tracks comment_ids that have been deleted. Checked before sending
    a DM — if the comment was deleted, we skip the send."""

    __tablename__ = "deleted_comments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id = Column(String, unique=True, nullable=False, index=True)
    received_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )


class DuplicateBlock(Base):
    """Explicit counter for duplicates_blocked. Each row = one blocked attempt.
    This lets /stats count accurately via SQL even after restarts."""

    __tablename__ = "duplicate_blocks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    rule_id = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    blocked_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )


def _set_sqlite_wal(dbapi_connection, connection_record):
    """Enable WAL mode and set a busy timeout for SQLite.
    WAL allows concurrent readers and a single writer without readers blocking."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


def init_db(db_path: str = "linkplease.db") -> sessionmaker:
    """Create the engine, apply WAL pragmas, create all tables, return a Session factory."""
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
    event.listen(engine, "connect", _set_sqlite_wal)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
