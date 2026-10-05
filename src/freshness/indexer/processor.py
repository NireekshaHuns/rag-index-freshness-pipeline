"""Brings one document's chunks in line with its current database state."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg
from psycopg_pool import ConnectionPool

from freshness.chunking import DEFAULT_CONFIG, Chunk, ChunkingConfig, chunk_document
from freshness.diffing import diff_chunks
from freshness.embeddings import EmbeddingProvider
from freshness.locks import lock_document
from freshness.vectors import to_pgvector

log = logging.getLogger(__name__)

Outcome = Literal["indexed", "skipped_stale", "deleted"]


@dataclass(frozen=True)
class ProcessResult:
    outcome: Outcome
    document_version: int | None = None
    chunks_embedded: int = 0
    chunks_reused: int = 0
    chunks_deleted: int = 0
    committed_at: datetime | None = None


class ConcurrentModificationError(Exception):
    """The document kept changing underneath us; safe to retry later."""


class _Retry(Exception):
    pass


@dataclass(frozen=True)
class _Snapshot:
    version: int
    deleted: bool
    content: str
    indexed_version: int | None


class DocumentIndexer:
    def __init__(
        self,
        pool: ConnectionPool,
        provider: EmbeddingProvider,
        chunking: ChunkingConfig = DEFAULT_CONFIG,
        max_attempts: int = 3,
    ) -> None:
        self.pool = pool
        self.provider = provider
        self.chunking = chunking
        self.max_attempts = max_attempts

    def process(self, document_id: uuid.UUID) -> ProcessResult:
        """Index the document's *current* version. The triggering event is only a
        hint that something changed, so duplicates and reordering are harmless."""
        for attempt in range(1, self.max_attempts + 1):
            try:
                return self._process_once(document_id)
            except _Retry:
                log.info("document %s changed mid-index (attempt %d)", document_id, attempt)
        raise ConcurrentModificationError(f"document {document_id} kept changing")

    def _process_once(self, document_id: uuid.UUID) -> ProcessResult:
        with self.pool.connection() as conn:
            snapshot = _read_snapshot(conn, document_id)
            if snapshot is None or _already_indexed(snapshot):
                return ProcessResult("skipped_stale")
            if snapshot.deleted:
                return self._apply_delete(conn, document_id, snapshot.version)

            desired = chunk_document(snapshot.content, self.chunking)
            # Embedding is slow and may fail, so it happens before any locks
            # are taken; the transaction below re-validates before writing.
            plan = diff_chunks(desired, _stored_positions(conn, document_id))
            vectors = self.provider.embed([c.text for c in plan.to_embed]) if plan.to_embed else []
            embedded = {c.content_hash: v for c, v in zip(plan.to_embed, vectors, strict=True)}
            return self._apply_upsert(conn, document_id, snapshot.version, desired, embedded)

    def _apply_delete(
        self, conn: psycopg.Connection, document_id: uuid.UUID, version: int
    ) -> ProcessResult:
        with conn.transaction():
            current = _lock_and_recheck(conn, document_id, version)
            if current == "stale":
                return ProcessResult("skipped_stale")
            removed = conn.execute(
                "DELETE FROM chunks WHERE document_id = %s", (document_id,)
            ).rowcount
            committed_at = _record_indexed(conn, document_id, version, deleted=True)
        return ProcessResult("deleted", version, chunks_deleted=removed, committed_at=committed_at)

    def _apply_upsert(
        self,
        conn: psycopg.Connection,
        document_id: uuid.UUID,
        version: int,
        desired: list[Chunk],
        embedded: dict[str, list[float]],
    ) -> ProcessResult:
        with conn.transaction():
            current = _lock_and_recheck(conn, document_id, version)
            if current == "stale":
                return ProcessResult("skipped_stale")
            # Re-diff under the lock: if another writer touched this document's
            # chunks since we looked, we must not reuse an embedding that's gone.
            diff = diff_chunks(desired, _stored_positions(conn, document_id))
            if any(c.content_hash not in embedded for c in diff.to_embed):
                raise _Retry

            with conn.cursor() as cur:
                if diff.to_delete:
                    cur.execute(
                        "DELETE FROM chunks WHERE document_id = %s AND content_hash = ANY(%s)",
                        (document_id, diff.to_delete),
                    )
                if diff.to_embed:
                    cur.executemany(
                        """
                        INSERT INTO chunks (document_id, content_hash, position, content,
                                            embedding, document_version)
                        VALUES (%s, %s, %s, %s, %s::vector, %s)
                        """,
                        [
                            (
                                document_id,
                                c.content_hash,
                                c.position,
                                c.text,
                                to_pgvector(embedded[c.content_hash]),
                                version,
                            )
                            for c in diff.to_embed
                        ],
                    )
                if diff.repositioned:
                    cur.executemany(
                        """
                        UPDATE chunks SET position = %s
                        WHERE document_id = %s AND content_hash = %s
                        """,
                        [(c.position, document_id, c.content_hash) for c in diff.repositioned],
                    )
            committed_at = _record_indexed(conn, document_id, version, deleted=False)
        return ProcessResult(
            "indexed",
            version,
            chunks_embedded=len(diff.to_embed),
            chunks_reused=len(diff.to_reuse),
            chunks_deleted=len(diff.to_delete),
            committed_at=committed_at,
        )


def _read_snapshot(conn: psycopg.Connection, document_id: uuid.UUID) -> _Snapshot | None:
    row = conn.execute(
        """
        SELECT d.version, d.deleted, d.content, s.indexed_version
        FROM documents d LEFT JOIN index_state s ON s.document_id = d.id
        WHERE d.id = %s
        """,
        (document_id,),
    ).fetchone()
    return _Snapshot(*row) if row else None


def _already_indexed(snapshot: _Snapshot) -> bool:
    return snapshot.indexed_version is not None and snapshot.indexed_version >= snapshot.version


def _stored_positions(conn: psycopg.Connection, document_id: uuid.UUID) -> dict[str, int]:
    rows = conn.execute(
        "SELECT content_hash, position FROM chunks WHERE document_id = %s", (document_id,)
    ).fetchall()
    return {content_hash: position for content_hash, position in rows}


def _lock_and_recheck(
    conn: psycopg.Connection, document_id: uuid.UUID, version: int
) -> Literal["ok", "stale"]:
    """Serialize index writers per document, then confirm our version is still the
    one to write. Raises _Retry if the document moved on while we embedded."""
    lock_document(conn, document_id)
    row = conn.execute(
        """
        SELECT d.version, s.indexed_version
        FROM documents d LEFT JOIN index_state s ON s.document_id = d.id
        WHERE d.id = %s
        """,
        (document_id,),
    ).fetchone()
    assert row is not None  # documents are soft-deleted, never removed
    current_version, indexed_version = row
    if indexed_version is not None and indexed_version >= version:
        return "stale"
    if current_version != version:
        raise _Retry
    return "ok"


def _record_indexed(
    conn: psycopg.Connection, document_id: uuid.UUID, version: int, deleted: bool
) -> datetime:
    row = conn.execute(
        """
        INSERT INTO index_state (document_id, indexed_version, indexed_at, deleted)
        VALUES (%s, %s, clock_timestamp(), %s)
        ON CONFLICT (document_id) DO UPDATE
        SET indexed_version = EXCLUDED.indexed_version,
            indexed_at = EXCLUDED.indexed_at,
            deleted = EXCLUDED.deleted
        RETURNING indexed_at
        """,
        (document_id, version, deleted),
    ).fetchone()
    assert row is not None
    return row[0]
