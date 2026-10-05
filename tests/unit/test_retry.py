import random

import psycopg

from freshness.embeddings import PermanentEmbeddingError, TransientEmbeddingError
from freshness.indexer.processor import ConcurrentModificationError
from freshness.indexer.retry import RetryPolicy, is_retryable


def test_max_attempts_includes_the_first_try() -> None:
    assert RetryPolicy(max_retries=3).max_attempts == 4
    assert RetryPolicy(max_retries=0).max_attempts == 1


def test_delay_ceiling_doubles_each_retry() -> None:
    policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=1000, rng=random.Random(1))
    for retry in range(6):
        samples = [policy.delay(retry) for _ in range(500)]
        ceiling = 2**retry
        assert all(0 <= s <= ceiling for s in samples)
        # Full jitter: samples spread over the whole range, not clustered at the top.
        assert min(samples) < ceiling * 0.1 and max(samples) > ceiling * 0.9


def test_delay_is_capped() -> None:
    policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=5.0)
    assert all(policy.delay(20) <= 5.0 for _ in range(100))


def test_transient_failures_are_retried() -> None:
    assert is_retryable(TransientEmbeddingError("rate limited"))
    assert is_retryable(ConcurrentModificationError("busy"))
    assert is_retryable(psycopg.OperationalError("connection lost"))


def test_permanent_failures_are_not_retried() -> None:
    assert not is_retryable(PermanentEmbeddingError("bad api key"))
