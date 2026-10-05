"""Entry point: `python -m freshness.indexer`."""

import logging
import signal
import threading

from freshness.config import Settings
from freshness.db import create_pool, migrate
from freshness.embeddings import create_provider
from freshness.indexer.consumer import IndexerWorker, create_consumer
from freshness.indexer.processor import DocumentIndexer


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    migrate(settings.database_url, settings)
    pool = create_pool(settings, max_size=2)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    worker = IndexerWorker(
        create_consumer(settings),
        DocumentIndexer(pool, create_provider(settings)),
        settings.kafka_topic,
    )
    try:
        worker.run(stop)
    finally:
        pool.close()
