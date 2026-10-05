"""Kafka client construction shared by services."""

from confluent_kafka import Producer

from freshness.config import Settings


def create_producer(settings: Settings, **overrides: str | int | bool) -> Producer:
    config: dict[str, str | int | bool] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        # Idempotence gives acks=all and keeps per-partition order across
        # internal retries, so one document's events never swap places.
        "enable.idempotence": True,
        "linger.ms": 5,
        "delivery.timeout.ms": 30_000,
    }
    config.update(overrides)
    return Producer(config)
