"""Document persistence. Every write and its outbox event share one transaction."""

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.rows import class_row

from freshness import outbox


@dataclass(frozen=True)
class Document:
    id: uuid.UUID
    title: str
    content: str
    version: int
    deleted: bool
    updated_at: datetime


COLUMNS = "id, title, content, version, deleted, updated_at"


def create_document(conn: psycopg.Connection, title: str, content: str) -> Document:
    with conn.transaction(), conn.cursor(row_factory=class_row(Document)) as cur:
        doc = cur.execute(
            f"""
            INSERT INTO documents (id, title, content, version, updated_at)
            VALUES (%s, %s, %s, 1, now())
            RETURNING {COLUMNS}
            """,
            (uuid.uuid4(), title, content),
        ).fetchone()
        assert doc is not None
        outbox.enqueue(conn, doc.id, "upserted", doc.version)
    return doc


def update_document(
    conn: psycopg.Connection, document_id: uuid.UUID, title: str, content: str
) -> Document | None:
    """Returns None if the document doesn't exist or was deleted."""
    with conn.transaction(), conn.cursor(row_factory=class_row(Document)) as cur:
        # Incrementing in the UPDATE itself takes the row lock, so concurrent
        # writers get distinct, gap-free versions.
        doc = cur.execute(
            f"""
            UPDATE documents
            SET title = %s, content = %s, version = version + 1, updated_at = now()
            WHERE id = %s AND NOT deleted
            RETURNING {COLUMNS}
            """,
            (title, content, document_id),
        ).fetchone()
        if doc is not None:
            outbox.enqueue(conn, doc.id, "upserted", doc.version)
    return doc


def delete_document(conn: psycopg.Connection, document_id: uuid.UUID) -> Document | None:
    """Soft-deletes so the version history keeps advancing; the indexer needs a
    newer version than anything already indexed to know the delete wins."""
    with conn.transaction(), conn.cursor(row_factory=class_row(Document)) as cur:
        doc = cur.execute(
            f"""
            UPDATE documents
            SET deleted = true, version = version + 1, updated_at = now()
            WHERE id = %s AND NOT deleted
            RETURNING {COLUMNS}
            """,
            (document_id,),
        ).fetchone()
        if doc is not None:
            outbox.enqueue(conn, doc.id, "deleted", doc.version)
    return doc


def get_document(conn: psycopg.Connection, document_id: uuid.UUID) -> Document | None:
    with conn.cursor(row_factory=class_row(Document)) as cur:
        return cur.execute(
            f"SELECT {COLUMNS} FROM documents WHERE id = %s AND NOT deleted", (document_id,)
        ).fetchone()
