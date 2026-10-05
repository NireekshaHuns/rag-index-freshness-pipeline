"""Per-document advisory locks shared by everything that writes the index."""

import uuid

import psycopg


def lock_document(conn: psycopg.Connection, document_id: uuid.UUID) -> None:
    """Held until the surrounding transaction ends."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s::text, 0))", (document_id,))
