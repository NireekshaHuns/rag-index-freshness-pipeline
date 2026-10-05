"""Prometheus metrics for every service.

All series live in one module so names and labels stay consistent; each
process exposes the default registry on its own /metrics endpoint.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Outbox relay
OUTBOX_PUBLISHED = Counter(
    "outbox_events_published", "Outbox events acknowledged by Kafka and marked published."
)
OUTBOX_BACKLOG = Gauge("outbox_backlog", "Outbox rows not yet published.")

# Indexer
EVENTS_PROCESSED = Counter(
    "indexer_events_processed", "Change events handled by the indexer.", ["result"]
)
for _result in ("indexed", "skipped_stale", "deleted"):
    EVENTS_PROCESSED.labels(result=_result)

CHUNKS_EMBEDDED = Counter("indexer_chunks_embedded", "Chunks sent to the embedding provider.")
CHUNKS_REUSED = Counter("indexer_chunks_reused", "Unchanged chunks whose embedding was kept.")
CHUNKS_DELETED = Counter("indexer_chunks_deleted", "Chunks removed from the index.")
EMBEDDING_ERRORS = Counter("indexer_embedding_errors", "Failed embedding provider calls.")
RETRIES = Counter("indexer_retries", "Event processing attempts that were retried.")
DLQ_EVENTS = Counter("indexer_dlq_events", "Events sent to the dead-letter topic.")
CONSUMER_LAG = Gauge(
    "indexer_consumer_lag", "Messages behind the partition high watermark.", ["partition"]
)

FRESHNESS_LAG = Histogram(
    "index_freshness_lag_seconds",
    "Time from a document change being committed to its index transaction committing.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800),
)

# Reconciler
STALE_DOCUMENTS = Gauge(
    "reconciler_stale_documents", "Documents whose indexed version is behind the source."
)
ORPHAN_CHUNKS = Gauge(
    "reconciler_orphan_chunks", "Chunks found belonging to missing or deleted documents."
)
REQUEUED = Counter("reconciler_requeued", "Stale documents re-enqueued through the outbox.")


def serve(port: int) -> None:
    """Expose /metrics for a non-HTTP service (relay, indexer, reconciler)."""
    start_http_server(port)
