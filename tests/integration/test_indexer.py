import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.chunking import chunk_document
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.indexer.consumer import IndexerWorker, create_consumer
from freshness.indexer.dlq import DeadLetterPublisher
from freshness.indexer.processor import ConcurrentModificationError, DocumentIndexer
from freshness.kafka import create_producer
from freshness.relay.relay import OutboxRelay

DIMENSION = Settings().embedding_dimension


class RecordingProvider:
    """Fake embeddings that remember what they were asked to embed."""

    def __init__(self, before_embed: Callable[[int], None] | None = None) -> None:
        self.inner = FakeEmbeddingProvider(DIMENSION)
        self.calls: list[list[str]] = []
        self.before_embed = before_embed

    @property
    def dimension(self) -> int:
        return DIMENSION

    @property
    def embedded(self) -> list[str]:
        return [text for call in self.calls for text in call]

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        if self.before_embed:
            self.before_embed(len(self.calls))
        return self.inner.embed(texts)


def para(label: str) -> str:
    return " ".join(f"The {label} paragraph says thing {i}." for i in range(6))


def doc(*labels: str) -> str:
    return "\n\n".join(para(label) for label in labels)


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=4)
    yield pool
    pool.close()


@pytest.fixture
def provider() -> RecordingProvider:
    return RecordingProvider()


@pytest.fixture
def indexer(pool: ConnectionPool, provider: RecordingProvider) -> DocumentIndexer:
    return DocumentIndexer(pool, provider)


def fetch(pool: ConnectionPool, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def stored_chunks(pool: ConnectionPool, doc_id: uuid.UUID) -> list[tuple[int, str, int]]:
    return fetch(
        pool,
        "SELECT id, content, position FROM chunks WHERE document_id = %s ORDER BY position",
        (doc_id,),
    )


def index_state(pool: ConnectionPool, doc_id: uuid.UUID) -> tuple[int, bool] | None:
    rows = fetch(
        pool,
        "SELECT indexed_version, deleted FROM index_state WHERE document_id = %s",
        (doc_id,),
    )
    return rows[0] if rows else None  # type: ignore[return-value]


def create(pool: ConnectionPool, content: str) -> store.Document:
    with pool.connection() as conn:
        return store.create_document(conn, "Doc", content)


def update(pool: ConnectionPool, doc_id: uuid.UUID, content: str) -> None:
    with pool.connection() as conn:
        assert store.update_document(conn, doc_id, "Doc", content) is not None


def test_create_indexes_every_chunk(
    pool: ConnectionPool, indexer: DocumentIndexer, provider: RecordingProvider
) -> None:
    document = create(pool, doc("a", "b", "c"))

    result = indexer.process(document.id)

    expected = chunk_document(document.content)
    assert result.outcome == "indexed"
    assert (result.chunks_embedded, result.chunks_reused) == (3, 0)
    assert [(c[1], c[2]) for c in stored_chunks(pool, document.id)] == [
        (c.text, c.position) for c in expected
    ]
    assert provider.embedded == [c.text for c in expected]
    assert index_state(pool, document.id) == (1, False)


def test_editing_one_paragraph_re_embeds_only_that_chunk(
    pool: ConnectionPool, indexer: DocumentIndexer, provider: RecordingProvider
) -> None:
    document = create(pool, doc("a", "b", "c"))
    indexer.process(document.id)
    before = stored_chunks(pool, document.id)
    provider.calls.clear()

    update(pool, document.id, doc("a", "b edited", "c"))
    result = indexer.process(document.id)

    after = stored_chunks(pool, document.id)
    assert provider.embedded == [para("b edited")]
    assert (result.chunks_embedded, result.chunks_reused, result.chunks_deleted) == (1, 2, 1)
    # Unchanged chunks are the very same rows, so their embeddings were kept.
    assert after[0][0] == before[0][0] and after[2][0] == before[2][0]
    assert after[1][1] == para("b edited")
    assert index_state(pool, document.id) == (2, False)


def test_inserting_a_paragraph_moves_but_does_not_re_embed(
    pool: ConnectionPool, indexer: DocumentIndexer, provider: RecordingProvider
) -> None:
    document = create(pool, doc("a", "b"))
    indexer.process(document.id)
    provider.calls.clear()

    update(pool, document.id, doc("new", "a", "b"))
    indexer.process(document.id)

    assert provider.embedded == [para("new")]
    assert [(c[1], c[2]) for c in stored_chunks(pool, document.id)] == [
        (para("new"), 0),
        (para("a"), 1),
        (para("b"), 2),
    ]


def test_delete_removes_all_chunks(pool: ConnectionPool, indexer: DocumentIndexer) -> None:
    document = create(pool, doc("a", "b"))
    indexer.process(document.id)
    with pool.connection() as conn:
        store.delete_document(conn, document.id)

    result = indexer.process(document.id)

    assert result.outcome == "deleted"
    assert result.chunks_deleted == 2
    assert stored_chunks(pool, document.id) == []
    assert index_state(pool, document.id) == (2, True)


def test_same_event_twice_changes_nothing(
    pool: ConnectionPool, indexer: DocumentIndexer, provider: RecordingProvider
) -> None:
    document = create(pool, doc("a", "b"))
    indexer.process(document.id)
    before = stored_chunks(pool, document.id)
    calls_before = len(provider.calls)

    result = indexer.process(document.id)

    assert result.outcome == "skipped_stale"
    assert len(provider.calls) == calls_before
    assert stored_chunks(pool, document.id) == before
    assert index_state(pool, document.id) == (1, False)


def test_older_event_after_newer_is_skipped(pool: ConnectionPool, indexer: DocumentIndexer) -> None:
    document = create(pool, doc("v1"))
    update(pool, document.id, doc("v2"))

    # The v2 event arrives first and indexes the current (v2) content...
    assert indexer.process(document.id).outcome == "indexed"
    # ...then the delayed v1 event is a no-op rather than a rollback.
    assert indexer.process(document.id).outcome == "skipped_stale"

    assert [c[1] for c in stored_chunks(pool, document.id)] == [para("v2")]
    assert index_state(pool, document.id) == (2, False)


def test_late_upsert_cannot_resurrect_a_deleted_document(
    pool: ConnectionPool, indexer: DocumentIndexer
) -> None:
    document = create(pool, doc("a"))
    with pool.connection() as conn:
        store.delete_document(conn, document.id)

    # Whatever event arrives, the indexer reads current state: deleted.
    assert indexer.process(document.id).outcome == "deleted"
    assert indexer.process(document.id).outcome == "skipped_stale"
    assert stored_chunks(pool, document.id) == []


def test_unknown_document_is_skipped(indexer: DocumentIndexer) -> None:
    assert indexer.process(uuid.uuid4()).outcome == "skipped_stale"


def test_edit_during_embedding_retries_with_latest_version(pool: ConnectionPool) -> None:
    document = create(pool, doc("v1"))

    def edit_on_first_call(call: int) -> None:
        if call == 1:
            update(pool, document.id, doc("v2"))

    provider = RecordingProvider(before_embed=edit_on_first_call)
    result = DocumentIndexer(pool, provider).process(document.id)

    assert result.outcome == "indexed"
    assert result.document_version == 2
    assert provider.embedded == [para("v1"), para("v2")]
    assert [c[1] for c in stored_chunks(pool, document.id)] == [para("v2")]


def test_gives_up_if_document_never_settles(pool: ConnectionPool) -> None:
    document = create(pool, doc("v0"))

    def always_edit(call: int) -> None:
        update(pool, document.id, doc(f"v{call}"))

    indexer = DocumentIndexer(pool, RecordingProvider(before_embed=always_edit), max_attempts=3)
    with pytest.raises(ConcurrentModificationError):
        indexer.process(document.id)
    assert stored_chunks(pool, document.id) == []
    assert index_state(pool, document.id) is None


def test_empty_document_indexes_with_no_chunks(
    pool: ConnectionPool, indexer: DocumentIndexer, provider: RecordingProvider
) -> None:
    document = create(pool, "")
    assert indexer.process(document.id).outcome == "indexed"
    assert provider.calls == []
    assert index_state(pool, document.id) == (1, False)


def committed_offsets(settings: Settings, partitions: int) -> int:
    """Total committed offset for the group; 0 while the coordinator isn't ready."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
        }
    )
    try:
        tps = [TopicPartition(settings.kafka_topic, p) for p in range(partitions)]
        return sum(max(tp.offset, 0) for tp in consumer.committed(tps, timeout=10))
    except KafkaException:
        return 0
    finally:
        consumer.close()


def test_end_to_end_through_kafka(
    database_url: str, kafka_bootstrap: str, kafka_topic: str, pool: ConnectionPool
) -> None:
    settings = Settings(
        database_url=database_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        kafka_topic=kafka_topic,
        kafka_consumer_group=f"indexer-{uuid.uuid4().hex[:6]}",
    )
    documents = [create(pool, doc(f"d{i}a", f"d{i}b")) for i in range(5)]
    update(pool, documents[0].id, doc("d0a", "d0b edited"))
    with pool.connection() as conn:
        store.delete_document(conn, documents[1].id)

    relay = OutboxRelay(pool, create_producer(settings), kafka_topic)
    while relay.publish_batch():
        pass

    stop = threading.Event()
    worker = IndexerWorker(
        create_consumer(settings, **{"session.timeout.ms": 6000}),
        DocumentIndexer(pool, FakeEmbeddingProvider(DIMENSION)),
        kafka_topic,
        DeadLetterPublisher(create_producer(settings), f"{kafka_topic}.dlq"),
        stop=stop,
    )
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and committed_offsets(settings, 4) < 7:
            time.sleep(0.5)
    finally:
        stop.set()
        thread.join(timeout=15)

    assert committed_offsets(settings, 4) == 7  # 5 creates + 1 update + 1 delete
    assert index_state(pool, documents[0].id) == (2, False)
    assert [c[1] for c in stored_chunks(pool, documents[0].id)] == [para("d0a"), para("d0b edited")]
    assert index_state(pool, documents[1].id) == (2, True)
    assert stored_chunks(pool, documents[1].id) == []
    for document in documents[2:]:
        assert index_state(pool, document.id) == (1, False)
        assert len(stored_chunks(pool, document.id)) == 2
