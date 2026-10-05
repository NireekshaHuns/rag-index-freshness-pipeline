"""Fault-injection scenarios. Each ends with the full consistency verifier.

Run with `make chaos`. The services are real subprocesses; "kill" is SIGKILL.
"""

import random
import threading
import time

from harness import (
    Background,
    Pipeline,
    all_events,
    dead_lettered,
    index_snapshot,
    publish,
    wait_consistent,
    wait_consumed,
    wait_for,
)
from psycopg_pool import ConnectionPool

from freshness.config import Settings
from freshness.events import ChangeEvent


def test_indexer_killed_mid_run(
    pool: ConnectionPool, settings: Settings, pipeline: Pipeline
) -> None:
    pipeline.start("relay")
    first = pipeline.start("indexer", "indexer-a")
    second = pipeline.start("indexer", "indexer-b")
    # Warm up until both workers own partitions and are processing.
    Background(pool, seed=10, documents=20, operations=20, duration_seconds=1).result_after_start()
    wait_for(lambda: first.events_handled() > 0 and second.events_handled() > 0, 60)

    workload = Background(pool, seed=1, duration_seconds=12)
    workload.start()
    for victim in (first, second):
        handled = victim.events_handled()
        wait_for(lambda v=victim, h=handled: v.events_handled() > h + 5, 30)
        victim.kill()  # mid-stream: uncommitted offsets and open transactions die with it
        time.sleep(1)
        pipeline.restart(victim)
        time.sleep(2)
    assert workload.is_alive(), "both kills must land while writes are still arriving"

    stats = workload.result()
    report = wait_consistent(pool, settings, pipeline)

    assert report.live_documents + report.deleted_documents == stats.created + 20
    assert dead_lettered(settings) == 0


def test_duplicate_events(pool: ConnectionPool, settings: Settings, pipeline: Pipeline) -> None:
    pipeline.start("relay")
    pipeline.start("indexer")
    pipeline.start("indexer")
    Background(pool, seed=2, duration_seconds=3).result_after_start()
    wait_consistent(pool, settings, pipeline)
    wait_consumed(settings)
    before = index_snapshot(pool)

    # Every event again, twice over: once straight to Kafka, once via the relay
    # as if it had crashed after publishing but before marking rows published.
    events = all_events(pool)
    publish(settings, events)
    with pool.connection() as conn:
        conn.execute("UPDATE outbox SET published_at = NULL")
    wait_consumed(settings)
    wait_consistent(pool, settings, pipeline)

    assert index_snapshot(pool) == before, "duplicates must not change the index"
    assert dead_lettered(settings) == 0


def test_out_of_order_events(pool: ConnectionPool, settings: Settings, pipeline: Pipeline) -> None:
    pipeline.start("indexer")
    pipeline.start("indexer")
    rng = random.Random(3)
    stop = threading.Event()

    def shuffling_relay() -> None:
        """Publishes the outbox in random order, scattered across partitions, so
        one document's versions race each other on different workers."""
        while not stop.is_set():
            with pool.connection() as conn, conn.transaction():
                rows = conn.execute(
                    """
                    SELECT event_id, document_id, event_type, document_version, created_at
                    FROM outbox WHERE published_at IS NULL
                    ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED
                    """
                ).fetchall()
                events = [ChangeEvent(*row) for row in rows]
                rng.shuffle(events)
                if events:
                    publish(settings, events, random_keys=True)
                    conn.execute(
                        "UPDATE outbox SET published_at = now() WHERE event_id = ANY(%s)",
                        ([e.event_id for e in events],),
                    )
            stop.wait(0.2)

    relay = threading.Thread(target=shuffling_relay, daemon=True)
    relay.start()
    try:
        Background(pool, seed=3).result_after_start()
        wait_consistent(pool, settings, pipeline)
    finally:
        stop.set()
        relay.join(timeout=10)

    # Replaying the whole history backwards must be a no-op.
    wait_consumed(settings)
    before = index_snapshot(pool)
    publish(settings, reversed(all_events(pool)), random_keys=True)
    wait_consumed(settings)

    assert index_snapshot(pool) == before, "stale events must not change the index"
    wait_consistent(pool, settings, pipeline)
    assert dead_lettered(settings) == 0


def test_thirty_percent_embedding_failures(
    pool: ConnectionPool, settings: Settings, pipeline: Pipeline
) -> None:
    # Few retries on purpose, so some events exhaust them and are dead-lettered;
    # the reconciler must then bring those documents back in line.
    pipeline.extra_env.update(
        {
            "EMBEDDING_FAILURE_RATE": "0.3",
            "MAX_RETRIES": "2",
            "RECONCILE_INTERVAL_SECONDS": "2",
            "RECONCILE_GRACE_SECONDS": "2",
        }
    )
    pipeline.start("relay")
    pipeline.start("indexer")
    pipeline.start("indexer")
    pipeline.start("reconciler")

    stats = Background(pool, seed=4).result_after_start()
    report = wait_consistent(pool, settings, pipeline, timeout=180)

    assert report.live_documents + report.deleted_documents == stats.created
    print(f"\n  dead-lettered then repaired by the reconciler: {dead_lettered(settings)}")


def test_relay_restarted_mid_run(
    pool: ConnectionPool, settings: Settings, pipeline: Pipeline
) -> None:
    relay = pipeline.start("relay")
    pipeline.start("indexer")
    pipeline.start("indexer")
    workload = Background(pool, seed=5)
    workload.start()

    published_before_kills = []
    for _ in range(3):
        time.sleep(1.5)
        published_before_kills.append(published_count(pool))
        relay.kill()  # possibly after producing but before marking rows published
        time.sleep(0.5)
        pipeline.restart(relay)

    stats = workload.result()
    report = wait_consistent(pool, settings, pipeline)

    # Each restarted relay picked up where the last one died.
    assert published_before_kills == sorted(published_before_kills)
    assert published_before_kills[0] > 0
    assert report.live_documents + report.deleted_documents == stats.created
    assert dead_lettered(settings) == 0


def published_count(pool: ConnectionPool) -> int:
    with pool.connection() as conn:
        row = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL").fetchone()
    assert row is not None
    return row[0]
