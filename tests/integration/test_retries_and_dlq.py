import json
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence

import pytest
from confluent_kafka import Consumer, Message
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings import create_provider
from freshness.indexer.consumer import IndexerWorker, create_consumer
from freshness.indexer.dlq import DeadLetterPublisher
from freshness.indexer.processor import DocumentIndexer
from freshness.indexer.retry import RetryPolicy
from freshness.kafka import create_producer
from freshness.relay.relay import OutboxRelay


@pytest.fixture
def settings(database_url: str, kafka_bootstrap: str, kafka_topic: str) -> Settings:
    dlq = f"{kafka_topic}.dlq"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    for future in admin.create_topics([NewTopic(dlq, num_partitions=1)]).values():
        future.result(timeout=30)
    return Settings(
        database_url=database_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        kafka_topic=kafka_topic,
        kafka_dlq_topic=dlq,
        kafka_consumer_group=f"indexer-{uuid.uuid4().hex[:6]}",
        max_retries=3,
        retry_base_delay_seconds=0.01,
    )


@pytest.fixture
def pool(settings: Settings) -> Iterator[ConnectionPool]:
    pool = create_pool(settings, max_size=4)
    yield pool
    pool.close()


def seed_and_publish(pool: ConnectionPool, settings: Settings, count: int) -> list[uuid.UUID]:
    with pool.connection() as conn:
        ids = [
            store.create_document(conn, f"doc {i}", f"Paragraph {i} about topic {i}.").id
            for i in range(count)
        ]
    relay = OutboxRelay(pool, create_producer(settings), settings.kafka_topic)
    while relay.publish_batch():
        pass
    return ids


def run_worker_until(
    worker: IndexerWorker, done: Callable[[], bool], timeout: float = 45.0
) -> None:
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not done():
            time.sleep(0.25)
    finally:
        worker.stop.set()
        thread.join(timeout=15)


def make_worker(settings: Settings, pool: ConnectionPool, seed: int) -> IndexerWorker:
    provider = create_provider(settings, rng=random.Random(seed))
    return IndexerWorker(
        create_consumer(settings, **{"session.timeout.ms": 6000}),
        DocumentIndexer(pool, provider),
        settings.kafka_topic,
        DeadLetterPublisher(create_producer(settings), settings.kafka_dlq_topic),
        RetryPolicy(settings.max_retries, settings.retry_base_delay_seconds),
    )


def read_topic(settings: Settings, topic: str, expected: int, timeout: float = 15) -> list[Message]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages: list[Message] = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and len(messages) < expected:
            msg = consumer.poll(0.5)
            if msg is not None and not msg.error():
                messages.append(msg)
    finally:
        consumer.close()
    return messages


def indexed_count(pool: ConnectionPool) -> int:
    with pool.connection() as conn:
        row = conn.execute("SELECT count(*) FROM index_state").fetchone()
    assert row is not None
    return row[0]


def headers(msg: Message) -> dict[str, str]:
    raw: Sequence[tuple[str, bytes]] = msg.headers() or []
    return {k: v.decode() for k, v in raw}


def test_always_failing_embeddings_land_in_dlq(pool: ConnectionPool, settings: Settings) -> None:
    settings = Settings(**{**settings.__dict__, "embedding_failure_rate": 1.0})
    ids = seed_and_publish(pool, settings, count=4)
    worker = make_worker(settings, pool, seed=1)
    dead: list[Message] = []

    def all_dead_lettered() -> bool:
        # Each read starts from the beginning, so replace rather than append.
        dead[:] = read_topic(settings, settings.kafka_dlq_topic, 4, timeout=2)
        return len(dead) >= 4

    run_worker_until(worker, all_dead_lettered)

    assert len(dead) == 4
    assert {uuid.UUID(json.loads(m.value())["document_id"]) for m in dead} == set(ids)
    for msg in dead:
        h = headers(msg)
        assert h["dlq.error_type"] == "TransientEmbeddingError"
        assert h["dlq.attempts"] == "4"
        assert h["dlq.source_topic"] == settings.kafka_topic
        assert "injected embedding failure" in h["dlq.traceback"]
        # The original event is forwarded untouched, ready to replay.
        assert msg.key() == json.loads(msg.value())["document_id"].encode()
    assert indexed_count(pool) == 0


def test_partial_failures_eventually_succeed(pool: ConnectionPool, settings: Settings) -> None:
    settings = Settings(**{**settings.__dict__, "embedding_failure_rate": 0.3})
    seed_and_publish(pool, settings, count=20)
    worker = make_worker(settings, pool, seed=7)

    run_worker_until(worker, lambda: indexed_count(pool) == 20)

    assert indexed_count(pool) == 20
    assert read_topic(settings, settings.kafka_dlq_topic, 1, timeout=3) == []
