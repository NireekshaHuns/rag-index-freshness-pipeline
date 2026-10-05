"""Retry policy: which failures to retry, and how long to wait."""

import random
from dataclasses import dataclass, field

from freshness.embeddings import PermanentEmbeddingError


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0
    rng: random.Random = field(default_factory=random.Random, compare=False)

    @property
    def max_attempts(self) -> int:
        return self.max_retries + 1

    def delay(self, retry: int) -> float:
        """Full jitter: uniform in [0, base * 2^retry], capped. Spreading retries
        out keeps many workers from hammering a recovering API in lockstep."""
        ceiling = min(self.max_delay_seconds, self.base_delay_seconds * 2**retry)
        return self.rng.uniform(0.0, ceiling)


def is_retryable(exc: BaseException) -> bool:
    # Anything not known to be permanent is retried; bounded attempts mean a
    # misclassified error still ends up in the DLQ rather than looping forever.
    return not isinstance(exc, PermanentEmbeddingError)
