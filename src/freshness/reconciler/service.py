"""Entry point: `python -m freshness.reconciler`."""

import logging
import signal
import threading

from freshness import metrics
from freshness.config import Settings
from freshness.db import create_pool, migrate
from freshness.reconciler.reconciler import Reconciler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    migrate(settings.database_url, settings)
    metrics.serve(settings.metrics_port)
    pool = create_pool(settings, max_size=2)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        Reconciler(pool, grace_seconds=settings.reconcile_grace_seconds).run(
            stop, settings.reconcile_interval_seconds
        )
    finally:
        pool.close()
