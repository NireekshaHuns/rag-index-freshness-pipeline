"""Deterministic, offline embeddings for development, tests, and CI."""

import hashlib
import math
import random
import re
from collections.abc import Sequence

from freshness.hashing import normalize

_TOKEN = re.compile(r"\w+")


class FakeEmbeddingProvider:
    """Feature-hashed bag of words: each token is hashed to a signed dimension.

    Deterministic like a pure text hash, but texts that share words land near
    each other, so search over fake embeddings still returns sensible results.
    """

    def __init__(self, dimension: int) -> None:
        if dimension <= 0:
            raise ValueError("dimension must be positive")
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        vector = [0.0] * self._dimension
        for token in _TOKEN.findall(normalize(text).lower()):
            digest = hashlib.sha256(token.encode()).digest()
            index = int.from_bytes(digest[:8], "big") % self._dimension
            vector[index] += 1.0 if digest[8] & 1 else -1.0
        if not any(vector):
            # No word tokens (or they cancelled out): fall back to a vector
            # seeded by the whole text so it's still non-zero and unique.
            seed = hashlib.sha256(normalize(text).encode()).digest()
            rng = random.Random(seed)
            vector = [rng.gauss(0.0, 1.0) for _ in range(self._dimension)]
        norm = math.sqrt(sum(v * v for v in vector))
        return [v / norm for v in vector]
