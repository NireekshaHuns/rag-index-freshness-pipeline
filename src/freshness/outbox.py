"""Writing change events to the transactional outbox."""

import uuid

import psycopg

from freshness.events import EventType


def enqueue(
    conn: psycopg.Connection, document_id: uuid.UUID, event_type: EventType, version: int
) -> uuid.UUID:
    """Insert an outbox row. Must run inside the caller's transaction to be atomic."""
    event_id = uuid.uuid4()
    conn.execute(
        """
        INSERT INTO outbox (event_id, document_id, event_type, document_version, created_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (event_id, document_id, event_type, version),
    )
    return event_id
