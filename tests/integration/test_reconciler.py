import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.events import ChangeEvent
from freshness.indexer.processor import DocumentIndexer
from freshness.reconciler.reconciler import RECONCILER_LOCK_ID, Reconciler
from freshness.vectors import to_pgvector

DIMENSION = Settings().embedding_dimension


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=4)
    yield pool
    pool.close()


@pytest.fixture
def indexer(pool: ConnectionPool) -> DocumentIndexer:
    return DocumentIndexer(pool, FakeEmbeddingProvider(DIMENSION))


@pytest.fixture
def reconciler(pool: ConnectionPool) -> Reconciler:
    return Reconciler(pool, grace_seconds=0)


def execute(pool: ConnectionPool, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with pool.connection() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if cur.description else []


def text(*labels: str) -> str:
    return "\n\n".join(" ".join(f"{label} sentence {i}." for i in range(8)) for label in labels)


def create(pool: ConnectionPool, content: str) -> store.Document:
    with pool.connection() as conn:
        return store.create_document(conn, "Doc", content)


def lose_pending_events(pool: ConnectionPool) -> None:
    """Simulate events that were published but never applied (e.g. dead-lettered)."""
    execute(pool, "UPDATE outbox SET published_at = now() WHERE published_at IS NULL")


def deliver_pending(pool: ConnectionPool, indexer: DocumentIndexer) -> list[ChangeEvent]:
    """Stand-in for relay + Kafka: apply every unpublished outbox event once."""
    rows = execute(
        pool,
        """
        UPDATE outbox SET published_at = now() WHERE published_at IS NULL
        RETURNING event_id, document_id, event_type, document_version, created_at
        """,
    )
    events = [ChangeEvent(*row) for row in rows]
    for event in events:
        indexer.process(event.document_id)
    return events


def in_sync(pool: ConnectionPool) -> bool:
    [(behind,)] = execute(
        pool,
        """
        SELECT count(*) FROM documents d LEFT JOIN index_state s ON s.document_id = d.id
        WHERE s.indexed_version IS DISTINCT FROM d.version
        """,
    )
    return behind == 0


def test_in_sync_index_needs_no_repair(
    pool: ConnectionPool, indexer: DocumentIndexer, reconciler: Reconciler
) -> None:
    document = create(pool, text("a"))
    deliver_pending(pool, indexer)

    report = reconciler.reconcile()

    assert (report.stale_documents, report.requeued, report.orphan_chunks) == (0, 0, 0)
    assert execute(pool, "SELECT count(*) FROM outbox WHERE document_id = %s", (document.id,)) == [
        (1,)
    ]


def test_lost_events_are_requeued_and_repaired(
    pool: ConnectionPool, indexer: DocumentIndexer, reconciler: Reconciler
) -> None:
    never_indexed = create(pool, text("a"))
    edited = create(pool, text("b"))
    deliver_pending(pool, indexer)
    with pool.connection() as conn:
        store.update_document(conn, edited.id, "Doc", text("b edited"))
    lose_pending_events(pool)
    execute(pool, "DELETE FROM index_state WHERE document_id = %s", (never_indexed.id,))
    execute(pool, "DELETE FROM chunks WHERE document_id = %s", (never_indexed.id,))
    assert not in_sync(pool)

    report = reconciler.reconcile()

    assert (report.stale_documents, report.requeued) == (2, 2)
    requeued = deliver_pending(pool, indexer)
    assert {(e.document_id, e.event_type, e.document_version) for e in requeued} == {
        (never_indexed.id, "upserted", 1),
        (edited.id, "upserted", 2),
    }
    assert in_sync(pool)
    contents = execute(pool, "SELECT content FROM chunks WHERE document_id = %s", (edited.id,))
    assert contents == [(text("b edited"),)]
    assert reconciler.reconcile().stale_documents == 0


def test_lost_delete_is_requeued_as_delete(
    pool: ConnectionPool, indexer: DocumentIndexer, reconciler: Reconciler
) -> None:
    document = create(pool, text("a"))
    deliver_pending(pool, indexer)
    with pool.connection() as conn:
        store.delete_document(conn, document.id)
    lose_pending_events(pool)

    assert reconciler.reconcile().requeued == 1
    [event] = deliver_pending(pool, indexer)

    assert (event.event_type, event.document_version) == ("deleted", 2)
    assert execute(pool, "SELECT count(*) FROM chunks") == [(0,)]
    assert in_sync(pool)


def test_recent_changes_are_left_to_the_event_path(
    pool: ConnectionPool, indexer: DocumentIndexer
) -> None:
    create(pool, text("a"))
    lose_pending_events(pool)

    report = Reconciler(pool, grace_seconds=300).reconcile()

    # Reported as stale, but too fresh to second-guess the normal pipeline.
    assert (report.stale_documents, report.requeued) == (1, 0)


def test_pending_outbox_event_is_not_duplicated(
    pool: ConnectionPool, reconciler: Reconciler
) -> None:
    create(pool, text("a"))  # its outbox row is still waiting for the relay

    report = reconciler.reconcile()

    assert (report.stale_documents, report.requeued) == (1, 0)
    assert execute(pool, "SELECT count(*) FROM outbox") == [(1,)]


def test_orphan_chunks_are_removed(
    pool: ConnectionPool, indexer: DocumentIndexer, reconciler: Reconciler
) -> None:
    live = create(pool, text("live"))
    deleted = create(pool, text("gone one", "gone two"))
    deliver_pending(pool, indexer)
    with pool.connection() as conn:
        store.delete_document(conn, deleted.id)
    deliver_pending(pool, indexer)
    # Desync: chunks reappear for a deleted document and for one that never existed.
    vector = to_pgvector(FakeEmbeddingProvider(DIMENSION).embed(["x"])[0])
    for doc_id, content_hash in [(deleted.id, "h1"), (deleted.id, "h2"), (uuid.uuid4(), "h3")]:
        execute(
            pool,
            """
            INSERT INTO chunks (document_id, content_hash, position, content, embedding,
                                document_version)
            VALUES (%s, %s, 0, 'stray', %s::vector, 1)
            """,
            (doc_id, content_hash, vector),
        )

    report = reconciler.reconcile()

    assert (report.orphan_chunks, report.orphan_chunks_deleted) == (3, 3)
    assert execute(pool, "SELECT DISTINCT document_id FROM chunks") == [(live.id,)]
    assert reconciler.reconcile().orphan_chunks == 0


def test_only_one_reconciler_runs_at_a_time(pool: ConnectionPool) -> None:
    create(pool, text("a"))
    lose_pending_events(pool)
    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (RECONCILER_LOCK_ID,))
        assert Reconciler(pool, grace_seconds=0).reconcile().skipped

    assert Reconciler(pool, grace_seconds=0).reconcile().requeued == 1


def test_run_loop_repairs_and_stops(pool: ConnectionPool, indexer: DocumentIndexer) -> None:
    create(pool, text("a"))
    lose_pending_events(pool)
    stop = threading.Event()
    thread = threading.Thread(target=Reconciler(pool, grace_seconds=0).run, args=(stop, 0.05))
    thread.start()
    try:
        for _ in range(100):
            if execute(pool, "SELECT count(*) FROM outbox WHERE published_at IS NULL")[0][0]:
                break
            stop.wait(0.05)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    deliver_pending(pool, indexer)
    assert in_sync(pool)
