"""Publishes outbox rows to Kafka with at-least-once delivery."""

import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import KafkaError, Message, Producer
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from freshness.events import ChangeEvent

log = logging.getLogger(__name__)

# Locks are held until Kafka confirms delivery, so other relays skip these rows
# instead of publishing them twice.
SELECT_BATCH = """
    SELECT event_id, document_id, event_type, document_version, created_at
    FROM outbox
    WHERE published_at IS NULL
    ORDER BY id
    LIMIT %s
    FOR UPDATE SKIP LOCKED
"""

MARK_PUBLISHED = "UPDATE outbox SET published_at = now() WHERE event_id = ANY(%s)"


@dataclass
class DeliveryTracker:
    delivered: list[ChangeEvent] = field(default_factory=list)
    failed: list[tuple[ChangeEvent, KafkaError]] = field(default_factory=list)

    def callback(self, event: ChangeEvent) -> Callable[[KafkaError | None, Message], None]:
        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err is None:
                self.delivered.append(event)
            else:
                self.failed.append((event, err))

        return on_delivery


class OutboxRelay:
    def __init__(
        self,
        pool: ConnectionPool,
        producer: Producer,
        topic: str,
        batch_size: int = 500,
        flush_timeout_seconds: float = 30.0,
    ) -> None:
        self.pool = pool
        self.producer = producer
        self.topic = topic
        self.batch_size = batch_size
        self.flush_timeout_seconds = flush_timeout_seconds

    def publish_batch(self) -> int:
        """Publish one batch. Returns how many rows were marked published."""
        with self.pool.connection() as conn, conn.transaction():
            with conn.cursor(row_factory=class_row(ChangeEvent)) as cur:
                events = cur.execute(SELECT_BATCH, (self.batch_size,)).fetchall()
            if not events:
                return 0

            tracker = DeliveryTracker()
            for event in events:
                self.producer.produce(
                    self.topic,
                    key=event.key,
                    value=event.to_json(),
                    headers={"event_id": str(event.event_id)},
                    on_delivery=tracker.callback(event),
                )
            remaining = self.producer.flush(self.flush_timeout_seconds)

            for event, err in tracker.failed:
                log.warning("delivery failed for event %s: %s", event.event_id, err)
            if remaining:
                log.warning("%d messages still unconfirmed after flush", remaining)

            # Only rows Kafka acknowledged are marked; the rest are retried next
            # round. If we crash before commit, everything here is resent, which
            # is safe because consumers are idempotent.
            delivered_ids = [event.event_id for event in tracker.delivered]
            if delivered_ids:
                self.mark_published(conn, delivered_ids)
            return len(delivered_ids)

    def mark_published(self, conn: psycopg.Connection, event_ids: list[uuid.UUID]) -> None:
        conn.execute(MARK_PUBLISHED, (event_ids,))

    def run(self, stop: threading.Event, poll_interval_seconds: float) -> None:
        log.info("outbox relay started, publishing to %s", self.topic)
        while not stop.is_set():
            try:
                published = self.publish_batch()
            except Exception:
                log.exception("relay batch failed; retrying")
                published = 0
            # Drain a backlog without pausing; only sleep when caught up.
            if published < self.batch_size:
                stop.wait(poll_interval_seconds)
        log.info("outbox relay stopped")
