"""Entry point: `python -m freshness.relay`."""

import logging
import signal
import threading

from freshness.config import Settings
from freshness.db import create_pool, migrate
from freshness.kafka import create_producer
from freshness.relay.relay import OutboxRelay


def install_stop_handlers(stop: threading.Event) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    migrate(settings.database_url, settings)
    pool = create_pool(settings, max_size=2)
    producer = create_producer(settings)
    stop = threading.Event()
    install_stop_handlers(stop)
    try:
        OutboxRelay(pool, producer, settings.kafka_topic).run(
            stop, settings.relay_poll_interval_ms / 1000
        )
    finally:
        producer.flush(10)
        pool.close()
