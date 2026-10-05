"""Kafka consumer loop: process an event, then commit its offset."""

import logging
import threading
import time

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from freshness import metrics
from freshness.config import Settings
from freshness.events import ChangeEvent
from freshness.indexer.dlq import DeadLetterError, DeadLetterPublisher
from freshness.indexer.processor import DocumentIndexer, ProcessResult
from freshness.indexer.retry import RetryPolicy, is_retryable

log = logging.getLogger(__name__)


def create_consumer(settings: Settings, **overrides: str | int | bool) -> Consumer:
    config: dict[str, str | int | bool] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "auto.offset.reset": "earliest",
        # Offsets are committed by hand, only after the index transaction commits.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "partition.assignment.strategy": "cooperative-sticky",
    }
    config.update(overrides)
    return Consumer(config)


class IndexerWorker:
    def __init__(
        self,
        consumer: Consumer,
        indexer: DocumentIndexer,
        topic: str,
        dead_letters: DeadLetterPublisher,
        retry: RetryPolicy | None = None,
        stop: threading.Event | None = None,
    ) -> None:
        self.consumer = consumer
        self.indexer = indexer
        self.topic = topic
        self.dead_letters = dead_letters
        self.retry = retry or RetryPolicy()
        self.stop = stop or threading.Event()
        self.lag_report_interval_seconds = 5.0
        self._lag_reported_at = 0.0
        self._lag_partitions: set[str] = set()

    def handle(self, event: ChangeEvent) -> ProcessResult:
        result = self.indexer.process(event.document_id)
        record_result(result)
        log.info(
            "event %s doc %s v%d -> %s (embedded=%d reused=%d deleted=%d)",
            event.event_id,
            event.document_id,
            event.document_version,
            result.outcome,
            result.chunks_embedded,
            result.chunks_reused,
            result.chunks_deleted,
        )
        return result

    def poll_once(self, timeout: float = 1.0) -> bool:
        """Handle at most one message. Returns True if its offset was committed."""
        msg = self.consumer.poll(timeout)
        if msg is None:
            return False
        if msg.error():
            log.warning("consumer error: %s", msg.error())
            return False
        try:
            event = ChangeEvent.from_json(msg.value())
        except (ValueError, KeyError) as exc:
            # A malformed event can never succeed; retrying would only stall the partition.
            log.error("malformed event at %s: %s", _position(msg), exc)
            return self._dead_letter(msg, exc, attempts=1)

        for attempt in range(1, self.retry.max_attempts + 1):
            try:
                self.handle(event)
            except Exception as exc:
                if not is_retryable(exc) or attempt == self.retry.max_attempts:
                    log.error(
                        "event %s failed after %d attempt(s): %r", event.event_id, attempt, exc
                    )
                    return self._dead_letter(msg, exc, attempts=attempt)
                delay = self.retry.delay(attempt - 1)
                metrics.RETRIES.inc()
                log.warning(
                    "event %s attempt %d failed (%r); retrying in %.2fs",
                    event.event_id,
                    attempt,
                    exc,
                    delay,
                )
                # Retry in place so later events for this document stay behind it.
                if self.stop.wait(delay):
                    self._rewind(msg)  # shutting down: leave it for the next owner
                    return False
            else:
                self._commit(msg)
                return True
        raise AssertionError("unreachable")

    def _dead_letter(self, msg: Message, error: BaseException, attempts: int) -> bool:
        try:
            self.dead_letters.publish(msg, error, attempts)
        except DeadLetterError:
            log.exception("could not dead-letter %s; will redeliver", _position(msg))
            self._rewind(msg)
            self.stop.wait(1.0)
            return False
        metrics.DLQ_EVENTS.inc()
        # Committing past the poison event unblocks the partition.
        self._commit(msg)
        return True

    def run(self) -> None:
        self.consumer.subscribe([self.topic])
        log.info("indexer consuming %s", self.topic)
        try:
            while not self.stop.is_set():
                self.poll_once()
                self._maybe_report_lag()
        finally:
            self.consumer.close()
            log.info("indexer stopped")

    def _maybe_report_lag(self) -> None:
        now = time.monotonic()
        if now - self._lag_reported_at < self.lag_report_interval_seconds:
            return
        self._lag_reported_at = now
        try:
            self._lag_partitions = report_consumer_lag(self.consumer, self._lag_partitions)
        except KafkaException:
            log.debug("could not read consumer lag", exc_info=True)

    def _commit(self, msg: Message) -> None:
        self.consumer.commit(message=msg, asynchronous=False)

    def _rewind(self, msg: Message) -> None:
        """Seek back so the same message is redelivered; committing a later offset
        would otherwise silently skip this one."""
        partition = TopicPartition(msg.topic(), msg.partition(), msg.offset())
        try:
            self.consumer.seek(partition)
        except KafkaException:
            log.exception("seek failed for %s", _position(msg))


def record_result(result: ProcessResult) -> None:
    metrics.EVENTS_PROCESSED.labels(result=result.outcome).inc()
    metrics.CHUNKS_EMBEDDED.inc(result.chunks_embedded)
    metrics.CHUNKS_REUSED.inc(result.chunks_reused)
    metrics.CHUNKS_DELETED.inc(result.chunks_deleted)
    lag = result.freshness_lag_seconds
    if lag is not None:
        metrics.FRESHNESS_LAG.observe(max(lag, 0.0))


def report_consumer_lag(consumer: Consumer, reported: set[str]) -> set[str]:
    """Lag per assigned partition: high watermark minus our current position.

    Values are gathered first and the gauge updated in place, so a scrape never
    sees a half-cleared set. Returns the partitions now reported.
    """
    lags: dict[str, int] = {}
    for tp in consumer.position(consumer.assignment()):
        _low, high = consumer.get_watermark_offsets(tp, timeout=2, cached=False)
        position = tp.offset if tp.offset >= 0 else high
        lags[str(tp.partition)] = max(high - position, 0)
    for partition, lag in lags.items():
        metrics.CONSUMER_LAG.labels(partition=partition).set(lag)
    for partition in reported - lags.keys():
        metrics.CONSUMER_LAG.remove(partition)  # revoked from this worker
    return set(lags)


def _position(msg: Message) -> str:
    return f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
