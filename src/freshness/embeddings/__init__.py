"""Pluggable embedding providers."""

import random

from freshness.config import Settings
from freshness.embeddings.base import (
    EmbeddingError,
    EmbeddingProvider,
    PermanentEmbeddingError,
    TransientEmbeddingError,
)
from freshness.embeddings.failing import FailureInjectingProvider
from freshness.embeddings.fake import FakeEmbeddingProvider

__all__ = [
    "EmbeddingError",
    "EmbeddingProvider",
    "PermanentEmbeddingError",
    "TransientEmbeddingError",
    "create_provider",
]


def create_provider(settings: Settings, rng: random.Random | None = None) -> EmbeddingProvider:
    provider: EmbeddingProvider
    if settings.embedding_provider == "openai":
        from freshness.embeddings.openai import OpenAIEmbeddingProvider

        assert settings.openai_api_key  # enforced by Settings validation
        provider = OpenAIEmbeddingProvider(settings.openai_api_key, settings.embedding_dimension)
    else:
        provider = FakeEmbeddingProvider(settings.embedding_dimension)
    if settings.embedding_failure_rate > 0:
        provider = FailureInjectingProvider(provider, settings.embedding_failure_rate, rng)
    return provider
