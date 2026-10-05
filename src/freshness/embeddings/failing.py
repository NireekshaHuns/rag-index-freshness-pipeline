"""Failure injection for chaos testing."""

import random
from collections.abc import Sequence

from freshness.embeddings.base import EmbeddingProvider, TransientEmbeddingError


class FailureInjectingProvider:
    """Fails a fraction of calls with a transient error before delegating."""

    def __init__(
        self, inner: EmbeddingProvider, failure_rate: float, rng: random.Random | None = None
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be between 0 and 1")
        self._inner = inner
        self._failure_rate = failure_rate
        self._rng = rng or random.Random()

    @property
    def dimension(self) -> int:
        return self._inner.dimension

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if self._rng.random() < self._failure_rate:
            raise TransientEmbeddingError("injected embedding failure")
        return self._inner.embed(texts)
