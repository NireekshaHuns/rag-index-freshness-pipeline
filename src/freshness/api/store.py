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


@dataclass(frozen=True)
class IndexedChunk:
    position: int
    content: str
    embedded_at_version: int


@dataclass(frozen=True)
class IndexStatus:
    """How far one document has travelled through the pipeline, on the database clock."""

    document: Document
    published_at: datetime | None
    """When the current version's event was first published to Kafka."""
    indexed_version: int | None
    indexed_at: datetime | None
    chunks: list[IndexedChunk]


def get_index_status(conn: psycopg.Connection, document_id: uuid.UUID) -> IndexStatus | None:
    """Unlike get_document, includes deleted documents so a delete can be followed too."""
    with conn.cursor(row_factory=class_row(Document)) as cur:
        doc = cur.execute(
            f"SELECT {COLUMNS} FROM documents WHERE id = %s", (document_id,)
        ).fetchone()
    if doc is None:
        return None
    published = conn.execute(
        """
        SELECT min(published_at) FROM outbox
        WHERE document_id = %s AND document_version = %s
        """,
        (document_id, doc.version),
    ).fetchone()
    state = conn.execute(
        "SELECT indexed_version, indexed_at FROM index_state WHERE document_id = %s",
        (document_id,),
    ).fetchone()
    with conn.cursor(row_factory=class_row(IndexedChunk)) as cur:
        chunks = cur.execute(
            """
            SELECT position, content, document_version AS embedded_at_version
            FROM chunks WHERE document_id = %s ORDER BY position
            """,
            (document_id,),
        ).fetchall()
    return IndexStatus(
        document=doc,
        published_at=published[0] if published else None,
        indexed_version=state[0] if state else None,
        indexed_at=state[1] if state else None,
        chunks=chunks,
    )
