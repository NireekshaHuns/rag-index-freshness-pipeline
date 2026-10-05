import uuid
from collections.abc import Iterator, Sequence

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.api.app import create_app
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings import TransientEmbeddingError
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.indexer.consumer import record_result
from freshness.indexer.processor import DocumentIndexer
from freshness.kafka import create_producer
from freshness.reconciler.reconciler import Reconciler
from freshness.relay.relay import OutboxRelay
from metrics_helpers import sample

DIMENSION = Settings().embedding_dimension


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=4)
    yield pool
    pool.close()


def create(pool: ConnectionPool, content: str) -> uuid.UUID:
    with pool.connection() as conn:
        return store.create_document(conn, "Doc", content).id


def test_api_exposes_metrics(database_url: str) -> None:
    with TestClient(create_app(Settings(database_url=database_url))) as client:
        response = client.get("/metrics/")
    assert response.status_code == 200
    assert "outbox_backlog" in response.text
    assert "index_freshness_lag_seconds_bucket" in response.text


def test_relay_reports_published_and_backlog(
    pool: ConnectionPool, kafka_bootstrap: str, kafka_topic: str
) -> None:
    for i in range(3):
        create(pool, f"doc {i}")
    settings = Settings(kafka_bootstrap_servers=kafka_bootstrap, kafka_topic=kafka_topic)
    relay = OutboxRelay(pool, create_producer(settings), kafka_topic, batch_size=2)
    published = sample("outbox_events_published_total")

    assert relay.report_backlog() == 3
    assert sample("outbox_backlog") == 3
    relay.publish_batch()
    relay.publish_batch()
    relay.report_backlog()

    assert sample("outbox_events_published_total") == published + 3
    assert sample("outbox_backlog") == 0


def test_index_freshness_lag_is_measured_from_the_source_change(pool: ConnectionPool) -> None:
    doc_id = create(pool, "Some content worth indexing for the lag test.")
    count = sample("index_freshness_lag_seconds_count")

    result = DocumentIndexer(pool, FakeEmbeddingProvider(DIMENSION)).process(doc_id)
    record_result(result)

    assert result.freshness_lag_seconds is not None and result.freshness_lag_seconds > 0
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT s.indexed_at - o.created_at FROM index_state s
            JOIN outbox o ON o.document_id = s.document_id WHERE s.document_id = %s
            """,
            (doc_id,),
        ).fetchone()
    assert row is not None
    assert result.freshness_lag_seconds == row[0].total_seconds()
    assert sample("index_freshness_lag_seconds_count") == count + 1


def test_embedding_errors_are_counted(pool: ConnectionPool) -> None:
    class Broken(FakeEmbeddingProvider):
        def embed(self, texts: Sequence[str]) -> list[list[float]]:
            raise TransientEmbeddingError("down")

    doc_id = create(pool, "Content that will fail to embed.")
    errors = sample("indexer_embedding_errors_total")

    with pytest.raises(TransientEmbeddingError):
        DocumentIndexer(pool, Broken(DIMENSION)).process(doc_id)

    assert sample("indexer_embedding_errors_total") == errors + 1


def test_reconciler_sets_gauges(pool: ConnectionPool) -> None:
    create(pool, "never indexed")
    with pool.connection() as conn:
        conn.execute("UPDATE outbox SET published_at = now()")
    requeued = sample("reconciler_requeued_total")

    Reconciler(pool, grace_seconds=0).reconcile()

    assert sample("reconciler_stale_documents") == 1
    assert sample("reconciler_orphan_chunks") == 0
    assert sample("reconciler_requeued_total") == requeued + 1
