import socket
import urllib.request
from datetime import UTC, datetime, timedelta

from prometheus_client import generate_latest

from freshness import metrics
from freshness.indexer.consumer import record_result
from freshness.indexer.processor import ProcessResult
from metrics_helpers import sample

EXPECTED_SERIES = [
    "outbox_events_published_total",
    "outbox_backlog",
    'indexer_events_processed_total{result="indexed"}',
    'indexer_events_processed_total{result="skipped_stale"}',
    'indexer_events_processed_total{result="deleted"}',
    "indexer_chunks_embedded_total",
    "indexer_chunks_reused_total",
    "indexer_chunks_deleted_total",
    "indexer_embedding_errors_total",
    "indexer_retries_total",
    "indexer_dlq_events_total",
    "index_freshness_lag_seconds_bucket",
    "index_freshness_lag_seconds_count",
    "reconciler_stale_documents",
    "reconciler_orphan_chunks",
    "reconciler_requeued_total",
]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_all_series_are_exported() -> None:
    output = generate_latest().decode()
    for series in EXPECTED_SERIES:
        assert series in output, series


def test_service_metrics_endpoint_serves_the_series() -> None:
    port = free_port()
    metrics.serve(port)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()
    for series in EXPECTED_SERIES:
        assert series in body, series


def test_record_result_counts_chunks_and_observes_lag() -> None:
    before = {
        name: sample(name)
        for name in (
            "indexer_chunks_embedded_total",
            "indexer_chunks_reused_total",
            "indexer_chunks_deleted_total",
            "index_freshness_lag_seconds_count",
            "index_freshness_lag_seconds_sum",
        )
    }
    indexed_before = sample("indexer_events_processed_total", result="indexed")
    changed = datetime(2026, 1, 1, tzinfo=UTC)

    record_result(
        ProcessResult(
            "indexed",
            2,
            chunks_embedded=1,
            chunks_reused=4,
            chunks_deleted=1,
            committed_at=changed + timedelta(seconds=1.5),
            changed_at=changed,
        )
    )

    assert sample("indexer_events_processed_total", result="indexed") == indexed_before + 1
    assert sample("indexer_chunks_embedded_total") == before["indexer_chunks_embedded_total"] + 1
    assert sample("indexer_chunks_reused_total") == before["indexer_chunks_reused_total"] + 4
    assert sample("indexer_chunks_deleted_total") == before["indexer_chunks_deleted_total"] + 1
    assert (
        sample("index_freshness_lag_seconds_count")
        == before["index_freshness_lag_seconds_count"] + 1
    )
    assert (
        sample("index_freshness_lag_seconds_sum") == before["index_freshness_lag_seconds_sum"] + 1.5
    )


def test_skipped_events_do_not_observe_lag() -> None:
    lag_before = sample("index_freshness_lag_seconds_count")
    skipped_before = sample("indexer_events_processed_total", result="skipped_stale")

    record_result(ProcessResult("skipped_stale"))

    assert sample("index_freshness_lag_seconds_count") == lag_before
    assert sample("indexer_events_processed_total", result="skipped_stale") == skipped_before + 1


def test_freshness_lag_property() -> None:
    now = datetime.now(UTC)
    result = ProcessResult("indexed", committed_at=now, changed_at=now - timedelta(seconds=2))
    assert result.freshness_lag_seconds == 2.0
    assert ProcessResult("skipped_stale").freshness_lag_seconds is None
