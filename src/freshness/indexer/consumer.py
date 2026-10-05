"""Kafka consumer loop: process an event, then commit its offset."""

import logging
import threading

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from freshness.config import Settings
from freshness.events import ChangeEvent
from freshness.indexer.processor import DocumentIndexer, ProcessResult

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
        failure_backoff_seconds: float = 1.0,
    ) -> None:
        self.consumer = consumer
        self.indexer = indexer
        self.topic = topic
        self.failure_backoff_seconds = failure_backoff_seconds

    def handle(self, event: ChangeEvent) -> ProcessResult:
        result = self.indexer.process(event.document_id)
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
        """Handle at most one message. Returns True if a message was committed."""
        msg = self.consumer.poll(timeout)
        if msg is None:
            return False
        if msg.error():
            log.warning("consumer error: %s", msg.error())
            return False
        try:
            event = ChangeEvent.from_json(msg.value())
        except (ValueError, KeyError) as exc:
            # Unparseable events can never succeed; skip rather than block the partition.
            log.error("dropping malformed event at %s: %s", _position(msg), exc)
            self._commit(msg)
            return True
        try:
            self.handle(event)
        except Exception:
            log.exception("failed to process event %s; will retry", event.event_id)
            # Rewind so the same message is redelivered; committing a later
            # offset would otherwise silently skip this one.
            self._rewind(msg)
            return False
        self._commit(msg)
        return True

    def run(self, stop: threading.Event) -> None:
        self.consumer.subscribe([self.topic])
        log.info("indexer consuming %s", self.topic)
        try:
            while not stop.is_set():
                self.poll_once()
        finally:
            self.consumer.close()
            log.info("indexer stopped")

    def _commit(self, msg: Message) -> None:
        self.consumer.commit(message=msg, asynchronous=False)

    def _rewind(self, msg: Message) -> None:
        partition = TopicPartition(msg.topic(), msg.partition(), msg.offset())
        try:
            self.consumer.seek(partition)
        except KafkaException:
            log.exception("seek failed for %s", _position(msg))
        threading.Event().wait(self.failure_backoff_seconds)


def _position(msg: Message) -> str:
    return f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
