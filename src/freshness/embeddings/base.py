"""Embedding provider interface and error types."""

from collections.abc import Sequence
from typing import Protocol


class EmbeddingError(Exception):
    """Base class for embedding failures."""


class TransientEmbeddingError(EmbeddingError):
    """Worth retrying: timeouts, rate limits, server errors."""


class PermanentEmbeddingError(EmbeddingError):
    """Retrying won't help: bad credentials, invalid input."""


class EmbeddingProvider(Protocol):
    @property
    def dimension(self) -> int: ...

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per input text, in input order."""
        ...


def check_dimensions(vectors: list[list[float]], expected_count: int, dimension: int) -> None:
    if len(vectors) != expected_count:
        raise PermanentEmbeddingError(f"expected {expected_count} vectors, got {len(vectors)}")
    for vector in vectors:
        if len(vector) != dimension:
            raise PermanentEmbeddingError(f"expected dimension {dimension}, got {len(vector)}")
