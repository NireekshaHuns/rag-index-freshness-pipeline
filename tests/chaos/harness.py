"""Runs the real services as subprocesses so faults are real process crashes."""

import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.config import Settings
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.events import ChangeEvent
from freshness.kafka import create_producer
from freshness.verify import VerifyReport, verify

MODULES = {
    "relay": "freshness.relay",
    "indexer": "freshness.indexer",
    "reconciler": "freshness.reconciler",
}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class Service:
    name: str
    kind: str
    env: dict[str, str]
    log_path: Path
    process: subprocess.Popen[bytes] | None = None

    def start(self) -> "Service":
        log = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [sys.executable, "-m", MODULES[self.kind]],
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        return self

    def kill(self) -> None:
        """SIGKILL: no shutdown hooks, no final commits, no flushed buffers."""
        assert self.process is not None
        self.process.send_signal(signal.SIGKILL)
        self.process.wait(timeout=10)

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def events_handled(self) -> int:
        """Indexer log lines for processed events, i.e. proof it was doing work."""
        if not self.log_path.exists():
            return 0
        return self.log_path.read_text(errors="replace").count(" -> ")

    def log_tail(self, lines: int = 25) -> str:
        if not self.log_path.exists():
            return ""
        return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-lines:])


@dataclass
class Pipeline:
    settings: Settings
    log_dir: Path
    extra_env: dict[str, str] = field(default_factory=dict)
    services: list[Service] = field(default_factory=list)

    def env(self, overrides: dict[str, str]) -> dict[str, str]:
        s = self.settings
        return {
            **os.environ,
            "DATABASE_URL": s.database_url,
            "KAFKA_BOOTSTRAP_SERVERS": s.kafka_bootstrap_servers,
            "KAFKA_TOPIC": s.kafka_topic,
            "KAFKA_DLQ_TOPIC": s.kafka_dlq_topic,
            "KAFKA_CONSUMER_GROUP": s.kafka_consumer_group,
            "KAFKA_SESSION_TIMEOUT_MS": "6000",
            "EMBEDDING_PROVIDER": "fake",
            "EMBEDDING_DIMENSION": str(s.embedding_dimension),
            "RELAY_POLL_INTERVAL_MS": "50",
            "RETRY_BASE_DELAY_SECONDS": "0.05",
            "METRICS_PORT": str(free_port()),
            **self.extra_env,
            **overrides,
        }

    def start(self, kind: str, name: str | None = None, **overrides: str) -> Service:
        name = name or f"{kind}-{len(self.services)}"
        service = Service(name, kind, self.env(overrides), self.log_dir / f"{name}.log")
        self.services.append(service.start())
        return service

    def restart(self, service: Service) -> Service:
        """Same role and log file, fresh process (and fresh consumer group member)."""
        service.env = self.env({"METRICS_PORT": str(free_port())})
        return service.start()

    def crashed(self) -> list[str]:
        """Services that exited on their own (deliberate kills are restarted)."""
        return [s.name for s in self.services if s.process and s.process.poll() is not None]

    def stop_all(self) -> None:
        for service in self.services:
            service.stop()

    def logs(self) -> str:
        return "\n\n".join(f"--- {s.name} ---\n{s.log_tail()}" for s in self.services)


PARAGRAPH_TOPICS = [
    "onboarding", "billing", "security", "refunds", "shipping", "privacy",
    "support", "pricing", "travel", "payroll", "hiring", "compliance",
]  # fmt: skip


def paragraph(rng: random.Random, topic: str) -> str:
    return " ".join(
        f"The {topic} rule number {rng.randrange(1000)} applies to case {rng.randrange(1000)}."
        for _ in range(rng.randint(3, 6))
    )


def document(rng: random.Random) -> str:
    return "\n\n".join(
        paragraph(rng, rng.choice(PARAGRAPH_TOPICS)) for _ in range(rng.randint(3, 8))
    )


def mutate(rng: random.Random, content: str) -> str:
    """Edit, insert, remove, or reorder paragraphs, like a person would."""
    paragraphs = content.split("\n\n")
    action = rng.choice(["edit", "edit", "insert", "remove", "swap"])
    index = rng.randrange(len(paragraphs))
    if action == "edit":
        paragraphs[index] = paragraph(rng, rng.choice(PARAGRAPH_TOPICS))
    elif action == "insert":
        paragraphs.insert(index, paragraph(rng, rng.choice(PARAGRAPH_TOPICS)))
    elif action == "remove" and len(paragraphs) > 1:
        paragraphs.pop(index)
    elif len(paragraphs) > 1:
        other = rng.randrange(len(paragraphs))
        paragraphs[index], paragraphs[other] = paragraphs[other], paragraphs[index]
    return "\n\n".join(paragraphs)


@dataclass
class WorkloadStats:
    created: int = 0
    edits: int = 0
    deletes: int = 0


def run_workload(
    pool: ConnectionPool,
    seed: int,
    documents: int = 40,
    operations: int = 160,
    duration_seconds: float = 6.0,
) -> WorkloadStats:
    """Create documents, then spread edits and deletes over `duration_seconds`."""
    rng = random.Random(seed)
    stats = WorkloadStats()
    live: dict[uuid.UUID, str] = {}
    with pool.connection() as conn:
        for i in range(documents):
            content = document(rng)
            live[store.create_document(conn, f"doc {seed}-{i}", content).id] = content
            stats.created += 1
        pause = duration_seconds / operations
        for _ in range(operations):
            doc_id = rng.choice(list(live))
            if rng.random() < 0.05 and len(live) > documents // 2:
                store.delete_document(conn, doc_id)
                del live[doc_id]
                stats.deletes += 1
            else:
                live[doc_id] = mutate(rng, live[doc_id])
                store.update_document(conn, doc_id, "edited", live[doc_id])
                stats.edits += 1
            time.sleep(pause)
    return stats


class Background(threading.Thread):
    """Runs the workload while the test injects faults, re-raising any error."""

    def __init__(self, pool: ConnectionPool, seed: int, **kwargs: int | float) -> None:
        super().__init__(daemon=True)
        self.pool, self.seed, self.kwargs = pool, seed, kwargs
        self.stats: WorkloadStats | None = None
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            self.stats = run_workload(self.pool, self.seed, **self.kwargs)  # type: ignore[arg-type]
        except BaseException as exc:
            self.error = exc

    def result_after_start(self, timeout: float = 60) -> WorkloadStats:
        self.start()
        return self.result(timeout)

    def result(self, timeout: float = 60) -> WorkloadStats:
        self.join(timeout)
        if self.error:
            raise self.error
        assert self.stats is not None, "workload did not finish"
        return self.stats


def wait_for(predicate: Callable[[], bool], timeout: float, interval: float = 0.2) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        time.sleep(interval)


def outbox_backlog(pool: ConnectionPool) -> int:
    with pool.connection() as conn:
        row = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()
    assert row is not None
    return row[0]


def check(pool: ConnectionPool, settings: Settings) -> VerifyReport:
    with pool.connection() as conn:
        return verify(conn, embeddings=FakeEmbeddingProvider(settings.embedding_dimension))


def wait_consistent(
    pool: ConnectionPool, settings: Settings, pipeline: Pipeline, timeout: float = 120
) -> VerifyReport:
    """Wait for the pipeline to drain, then require a clean verifier report."""
    deadline = time.monotonic() + timeout
    report = check(pool, settings)
    while time.monotonic() < deadline:
        if outbox_backlog(pool) == 0 and report.ok:
            return report
        time.sleep(1)
        report = check(pool, settings)
    raise AssertionError(
        f"index not consistent after {timeout}s: backlog={outbox_backlog(pool)} "
        f"{report.counts()}\n{[str(v) for v in report.violations[:10]]}\n{pipeline.logs()}"
    )


def committed_and_end_offsets(settings: Settings) -> tuple[int, int]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
        }
    )
    try:
        metadata = consumer.list_topics(settings.kafka_topic, timeout=10)
        partitions = [
            TopicPartition(settings.kafka_topic, p)
            for p in metadata.topics[settings.kafka_topic].partitions
        ]
        committed = sum(max(tp.offset, 0) for tp in consumer.committed(partitions, timeout=10))
        end = sum(consumer.get_watermark_offsets(tp, timeout=10)[1] for tp in partitions)
        return committed, end
    finally:
        consumer.close()


def wait_consumed(settings: Settings, timeout: float = 90) -> None:
    """Wait until the indexer group has committed every message on the topic."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            committed, end = committed_and_end_offsets(settings)
        except Exception:
            committed, end = -1, 0
        if committed == end:
            return
        time.sleep(1)
    raise AssertionError(f"indexer group did not catch up: committed={committed} end={end}")


def dead_lettered(settings: Settings) -> int:
    consumer = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": "dlq-count"}
    )
    try:
        tp = TopicPartition(settings.kafka_dlq_topic, 0)
        return consumer.get_watermark_offsets(tp, timeout=10)[1]
    finally:
        consumer.close()


def all_events(pool: ConnectionPool) -> list[ChangeEvent]:
    with pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT event_id, document_id, event_type, document_version, created_at
            FROM outbox ORDER BY id
            """
        ).fetchall()
    return [ChangeEvent(*row) for row in rows]


def publish(settings: Settings, events: Iterable[ChangeEvent], random_keys: bool = False) -> int:
    """Produce events straight to Kafka, bypassing the outbox.

    random_keys spreads one document's events across partitions, so different
    workers process them concurrently and in no particular order.
    """
    producer = create_producer(settings)
    count = 0
    for event in events:
        key = str(uuid.uuid4()).encode() if random_keys else event.key
        producer.produce(settings.kafka_topic, key=key, value=event.to_json())
        count += 1
        producer.poll(0)
    assert producer.flush(30) == 0
    return count


def index_snapshot(pool: ConnectionPool) -> list[tuple[object, ...]]:
    """Every chunk row and index_state row; any write would change this."""
    with pool.connection() as conn:
        chunks = conn.execute(
            "SELECT id, document_id, content_hash, position, document_version "
            "FROM chunks ORDER BY id"
        ).fetchall()
        states = conn.execute(
            "SELECT document_id, indexed_version, indexed_at, deleted FROM index_state "
            "ORDER BY document_id"
        ).fetchall()
    return [*chunks, *states]
