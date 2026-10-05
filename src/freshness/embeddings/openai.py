"""OpenAI embeddings (text-embedding-3-small)."""

from collections.abc import Sequence

import openai

from freshness.embeddings.base import (
    PermanentEmbeddingError,
    TransientEmbeddingError,
    check_dimensions,
)

MODEL = "text-embedding-3-small"
MAX_BATCH = 2048

_TRANSIENT = (
    openai.APIConnectionError,  # includes timeouts
    openai.RateLimitError,
    openai.InternalServerError,
    openai.ConflictError,
)


class OpenAIEmbeddingProvider:
    def __init__(self, api_key: str, dimension: int, client: openai.OpenAI | None = None) -> None:
        # The SDK's own retries are disabled; the indexer owns retry policy so
        # attempts are counted and bounded in one place.
        self._client = client or openai.OpenAI(api_key=api_key, max_retries=0, timeout=30.0)
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), MAX_BATCH):
            vectors.extend(self._embed_batch(list(texts[start : start + MAX_BATCH])))
        check_dimensions(vectors, len(texts), self._dimension)
        return vectors

    def _embed_batch(self, batch: list[str]) -> list[list[float]]:
        if not batch:
            return []
        try:
            response = self._client.embeddings.create(
                model=MODEL, input=batch, dimensions=self._dimension
            )
        except _TRANSIENT as exc:
            raise TransientEmbeddingError(str(exc)) from exc
        except openai.APIStatusError as exc:
            raise PermanentEmbeddingError(str(exc)) from exc
        return [item.embedding for item in sorted(response.data, key=lambda d: d.index)]
