"""Worker retry/DLQ control flow, with Kafka and the database stubbed out."""

import random
import threading
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from freshness.embeddings import PermanentEmbeddingError, TransientEmbeddingError
from freshness.events import ChangeEvent
from freshness.indexer.consumer import IndexerWorker
from freshness.indexer.dlq import DeadLetterError
from freshness.indexer.processor import ProcessResult
from freshness.indexer.retry import RetryPolicy


class StubMessage:
    def __init__(self, value: bytes, offset: int = 7) -> None:
        self._value = value
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes:
        return b"key"

    def topic(self) -> str:
        return "document-changes"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


class StubConsumer:
    def __init__(self, msg: StubMessage) -> None:
        self.msg = msg
        self.committed: list[int] = []
        self.seeks: list[int] = []

    def poll(self, timeout: float) -> StubMessage:
        return self.msg

    def commit(self, message: StubMessage, asynchronous: bool) -> None:
        self.committed.append(message.offset())

    def seek(self, partition: Any) -> None:
        self.seeks.append(partition.offset)


class StubIndexer:
    def __init__(self, failures: list[Exception]) -> None:
        self.failures = list(failures)
        self.calls = 0

    def process(self, document_id: uuid.UUID) -> ProcessResult:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return ProcessResult("indexed", 1)


class StubDeadLetters:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[tuple[StubMessage, BaseException, int]] = []

    def publish(self, msg: StubMessage, error: BaseException, attempts: int) -> None:
        if self.fail:
            raise DeadLetterError("broker down")
        self.published.append((msg, error, attempts))


def event_bytes() -> bytes:
    return ChangeEvent(uuid.uuid4(), uuid.uuid4(), "upserted", 1, datetime.now(UTC)).to_json()


def make_worker(
    failures: list[Exception],
    max_retries: int = 3,
    dlq_fails: bool = False,
    value: bytes | None = None,
    stop: threading.Event | None = None,
) -> tuple[IndexerWorker, StubConsumer, StubIndexer, StubDeadLetters]:
    consumer = StubConsumer(StubMessage(value or event_bytes()))
    indexer = StubIndexer(failures)
    dead_letters = StubDeadLetters(fail=dlq_fails)
    worker = IndexerWorker(
        consumer,  # type: ignore[arg-type]
        indexer,  # type: ignore[arg-type]
        "document-changes",
        dead_letters,  # type: ignore[arg-type]
        RetryPolicy(max_retries, base_delay_seconds=0.001, rng=random.Random(0)),
        stop,
    )
    return worker, consumer, indexer, dead_letters


def test_success_commits_without_dlq() -> None:
    worker, consumer, indexer, dlq = make_worker([])
    assert worker.poll_once() is True
    assert consumer.committed == [7]
    assert indexer.calls == 1 and dlq.published == []


def test_transient_failures_are_retried_until_success() -> None:
    worker, consumer, indexer, dlq = make_worker([TransientEmbeddingError("x")] * 2)
    assert worker.poll_once() is True
    assert indexer.calls == 3
    assert consumer.committed == [7] and dlq.published == []


def test_exhausted_retries_go_to_dlq_then_commit() -> None:
    worker, consumer, indexer, dlq = make_worker([TransientEmbeddingError("down")] * 10)
    assert worker.poll_once() is True
    assert indexer.calls == 4  # first try + 3 retries
    [(_, error, attempts)] = dlq.published
    assert isinstance(error, TransientEmbeddingError) and attempts == 4
    assert consumer.committed == [7]


def test_permanent_failure_skips_retries() -> None:
    worker, consumer, indexer, dlq = make_worker([PermanentEmbeddingError("bad key")])
    assert worker.poll_once() is True
    assert indexer.calls == 1
    assert dlq.published[0][2] == 1
    assert consumer.committed == [7]


def test_malformed_event_goes_straight_to_dlq() -> None:
    worker, consumer, indexer, dlq = make_worker([], value=b"not json")
    assert worker.poll_once() is True
    assert indexer.calls == 0
    assert len(dlq.published) == 1 and consumer.committed == [7]


def test_failed_dlq_write_does_not_commit() -> None:
    worker, consumer, _, _ = make_worker([PermanentEmbeddingError("x")], dlq_fails=True)
    worker.stop.wait = lambda timeout=None: False  # type: ignore[method-assign]
    assert worker.poll_once() is False
    assert consumer.committed == []
    assert consumer.seeks == [7]


def test_shutdown_during_backoff_rewinds_without_commit() -> None:
    stop = threading.Event()
    stop.set()
    worker, consumer, indexer, dlq = make_worker([TransientEmbeddingError("x")], stop=stop)
    assert worker.poll_once() is False
    assert indexer.calls == 1
    assert consumer.committed == [] and dlq.published == []
    assert consumer.seeks == [7]


@pytest.mark.parametrize("retries", [0, 1, 5])
def test_attempt_count_matches_policy(retries: int) -> None:
    worker, _, indexer, dlq = make_worker([TransientEmbeddingError("x")] * 10, max_retries=retries)
    worker.poll_once()
    assert indexer.calls == retries + 1
    assert dlq.published[0][2] == retries + 1
