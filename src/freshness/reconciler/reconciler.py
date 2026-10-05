"""Periodic safety net: finds index drift the event path missed and repairs it."""

import logging
import threading
import uuid
from dataclasses import dataclass

import psycopg
from psycopg_pool import ConnectionPool

from freshness import metrics, outbox
from freshness.locks import lock_document

log = logging.getLogger(__name__)

# Only one reconciler pass runs at a time across all replicas.
RECONCILER_LOCK_ID = 7_428_332

# Behind on its index, past the grace period, and with no event already queued.
# The grace period keeps normal in-flight lag from being double-enqueued.
FIND_STALE = """
    SELECT d.id, d.version, d.deleted
    FROM documents d
    LEFT JOIN index_state s ON s.document_id = d.id
    WHERE (s.indexed_version IS NULL OR s.indexed_version < d.version)
      AND d.updated_at < now() - make_interval(secs => %s)
      AND NOT EXISTS (
          SELECT 1 FROM outbox o
          WHERE o.document_id = d.id
            AND o.published_at IS NULL
            AND o.document_version >= d.version
      )
    ORDER BY d.updated_at
    LIMIT %s
"""

COUNT_STALE = """
    SELECT count(*)
    FROM documents d
    LEFT JOIN index_state s ON s.document_id = d.id
    WHERE s.indexed_version IS NULL OR s.indexed_version < d.version
"""

# Chunks whose document is gone or deleted. The indexer can't fix these when
# index_state already claims the delete was applied, so they're removed here.
FIND_ORPHANED_DOCUMENTS = """
    SELECT c.document_id, count(*)
    FROM chunks c
    LEFT JOIN documents d ON d.id = c.document_id
    WHERE d.id IS NULL OR d.deleted
    GROUP BY c.document_id
"""

DELETE_ORPHANS = """
    DELETE FROM chunks c
    WHERE c.document_id = %s
      AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = c.document_id AND NOT d.deleted)
"""


@dataclass(frozen=True)
class ReconcileReport:
    stale_documents: int
    requeued: int
    orphan_chunks: int
    orphan_chunks_deleted: int
    skipped: bool = False


class Reconciler:
    def __init__(
        self, pool: ConnectionPool, grace_seconds: float = 60.0, batch_size: int = 1000
    ) -> None:
        self.pool = pool
        self.grace_seconds = grace_seconds
        self.batch_size = batch_size

    def reconcile(self) -> ReconcileReport:
        with self.pool.connection() as conn:
            with conn.transaction():
                row = conn.execute(
                    "SELECT pg_try_advisory_xact_lock(%s)", (RECONCILER_LOCK_ID,)
                ).fetchone()
                if not (row and row[0]):
                    return ReconcileReport(0, 0, 0, 0, skipped=True)
                stale_count = _scalar(conn, COUNT_STALE)
                requeued = self._requeue_stale(conn)
            orphan_chunks, deleted = self._remove_orphans(conn)
        report = ReconcileReport(stale_count, requeued, orphan_chunks, deleted)
        metrics.STALE_DOCUMENTS.set(stale_count)
        metrics.ORPHAN_CHUNKS.set(orphan_chunks)
        metrics.REQUEUED.inc(requeued)
        log.info("reconcile: %s", report)
        return report

    def _requeue_stale(self, conn: psycopg.Connection) -> int:
        rows = conn.execute(FIND_STALE, (self.grace_seconds, self.batch_size)).fetchall()
        for document_id, version, deleted in rows:
            # Through the outbox, not straight to the indexer: repairs take the
            # same ordered, retried, observable path as every other change.
            outbox.enqueue(conn, document_id, "deleted" if deleted else "upserted", version)
            log.warning("requeued stale document %s at v%d", document_id, version)
        return len(rows)

    def _remove_orphans(self, conn: psycopg.Connection) -> tuple[int, int]:
        found = conn.execute(FIND_ORPHANED_DOCUMENTS).fetchall()
        deleted = 0
        for document_id, count in found:
            deleted += self._delete_orphans(conn, document_id)
            log.warning("removing %d orphan chunk(s) of document %s", count, document_id)
        return sum(count for _, count in found), deleted

    def _delete_orphans(self, conn: psycopg.Connection, document_id: uuid.UUID) -> int:
        with conn.transaction():
            lock_document(conn, document_id)
            return conn.execute(DELETE_ORPHANS, (document_id,)).rowcount

    def run(self, stop: threading.Event, interval_seconds: float) -> None:
        log.info("reconciler started, every %.0fs", interval_seconds)
        while not stop.is_set():
            try:
                self.reconcile()
            except Exception:
                log.exception("reconcile pass failed")
            stop.wait(interval_seconds)
        log.info("reconciler stopped")


def _scalar(conn: psycopg.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    assert row is not None
    return int(row[0])
