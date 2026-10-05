import threading
import uuid
from collections import defaultdict
from collections.abc import Iterator

import psycopg
import pytest
from confluent_kafka import Consumer
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.config import Settings
from freshness.db import create_pool
from freshness.events import ChangeEvent
from freshness.kafka import create_producer
from freshness.relay.relay import OutboxRelay


@pytest.fixture
def settings(database_url: str, kafka_bootstrap: str, kafka_topic: str) -> Settings:
    return Settings(
        database_url=database_url, kafka_bootstrap_servers=kafka_bootstrap, kafka_topic=kafka_topic
    )


@pytest.fixture
def pool(settings: Settings) -> Iterator[ConnectionPool]:
    pool = create_pool(settings, max_size=4)
    yield pool
    pool.close()


def make_relay(pool: ConnectionPool, settings: Settings, batch_size: int = 500) -> OutboxRelay:
    return OutboxRelay(pool, create_producer(settings), settings.kafka_topic, batch_size)


def write_changes(pool: ConnectionPool, documents: int, edits: int) -> list[uuid.UUID]:
    """Interleave edits across documents so per-document order is non-trivial."""
    with pool.connection() as conn:
        ids = [store.create_document(conn, f"doc {i}", "v1").id for i in range(documents)]
        for edit in range(edits):
            for doc_id in ids:
                store.update_document(conn, doc_id, "t", f"v{edit + 2}")
    return ids


def consume_all(settings: Settings, expected: int, timeout: float = 30.0) -> list[ChangeEvent]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])
    events: list[ChangeEvent] = []
    deadline = threading.Event()
    timer = threading.Timer(timeout, deadline.set)
    timer.start()
    try:
        while len(events) < expected and not deadline.is_set():
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            event = ChangeEvent.from_json(msg.value())
            assert msg.key() == event.key
            events.append(event)
        # Linger briefly so unexpected extra messages would be noticed.
        for _ in range(4):
            msg = consumer.poll(0.25)
            if msg is not None and not msg.error():
                events.append(ChangeEvent.from_json(msg.value()))
    finally:
        timer.cancel()
        consumer.close()
    return events


def outbox_ids(url: str, published: bool) -> set[uuid.UUID]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT event_id FROM outbox WHERE (published_at IS NOT NULL) = %s", (published,)
        ).fetchall()
    return {row[0] for row in rows}


def test_publishes_all_events_in_order_per_document(
    pool: ConnectionPool, settings: Settings
) -> None:
    write_changes(pool, documents=5, edits=6)
    relay = make_relay(pool, settings, batch_size=7)

    while relay.publish_batch():
        pass

    events = consume_all(settings, expected=35)
    assert len(events) == 35
    assert {e.event_id for e in events} == outbox_ids(settings.database_url, published=True)
    assert outbox_ids(settings.database_url, published=False) == set()
    versions: dict[uuid.UUID, list[int]] = defaultdict(list)
    for event in events:
        versions[event.document_id].append(event.document_version)
    assert all(v == list(range(1, 8)) for v in versions.values())


class Crash(Exception):
    pass


class CrashBeforeMarking(OutboxRelay):
    """Kafka has the messages, but the process dies before recording that."""

    def mark_published(self, conn: psycopg.Connection, event_ids: list[uuid.UUID]) -> None:
        raise Crash


def test_crash_before_marking_resends_on_restart(pool: ConnectionPool, settings: Settings) -> None:
    write_changes(pool, documents=3, edits=1)
    crashing = CrashBeforeMarking(pool, create_producer(settings), settings.kafka_topic)

    with pytest.raises(Crash):
        crashing.publish_batch()

    assert outbox_ids(settings.database_url, published=True) == set()

    restarted = make_relay(pool, settings)
    assert restarted.publish_batch() == 6

    events = consume_all(settings, expected=12)
    all_ids = outbox_ids(settings.database_url, published=True)
    # At-least-once: every event arrives, the crashed batch arrives twice.
    assert {e.event_id for e in events} == all_ids
    assert len(events) == 12


def test_stop_and_restart_loses_nothing(pool: ConnectionPool, settings: Settings) -> None:
    write_changes(pool, documents=4, edits=2)
    first = make_relay(pool, settings, batch_size=5)
    assert first.publish_batch() == 5
    # Relay goes away; more writes land while it's down.
    write_changes(pool, documents=2, edits=1)

    stop = threading.Event()
    second = make_relay(pool, settings, batch_size=5)
    thread = threading.Thread(target=second.run, args=(stop, 0.05))
    thread.start()
    try:
        events = consume_all(settings, expected=16)
    finally:
        stop.set()
        thread.join(timeout=10)

    assert len(events) == 16
    assert {e.event_id for e in events} == outbox_ids(settings.database_url, published=True)
    assert outbox_ids(settings.database_url, published=False) == set()


def test_concurrent_relays_do_not_double_publish(pool: ConnectionPool, settings: Settings) -> None:
    write_changes(pool, documents=10, edits=9)
    relays = [make_relay(pool, settings, batch_size=10) for _ in range(3)]
    stop = threading.Event()
    threads = [threading.Thread(target=r.run, args=(stop, 0.05)) for r in relays]
    for thread in threads:
        thread.start()
    try:
        events = consume_all(settings, expected=100)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)

    assert len(events) == 100
    assert len({e.event_id for e in events}) == 100
