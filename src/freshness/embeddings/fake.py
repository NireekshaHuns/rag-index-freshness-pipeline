"""Deterministic, offline embeddings for development, tests, and CI."""

import hashlib
import math
import random
import re
from collections import Counter
from collections.abc import Sequence

from freshness.hashing import normalize

_TOKEN = re.compile(r"\w+")

# Without these, filler words dominate short queries and rank unrelated text first.
STOP_WORDS = frozenset(
    [
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "can",
        "do",
        "does",
        "for",
        "from",
        "has",
        "have",
        "how",
        "if",
        "in",
        "is",
        "it",
        "its",
        "of",
        "on",
        "or",
        "our",
        "that",
        "the",
        "their",
        "there",
        "this",
        "to",
        "was",
        "we",
        "what",
        "when",
        "where",
        "which",
        "who",
        "why",
        "will",
        "with",
        "you",
        "your",
    ]
)


class FakeEmbeddingProvider:
    """Feature-hashed bag of words: each token is hashed to a signed dimension.

    Deterministic like a pure text hash, but texts that share meaningful words
    land near each other, so search over fake embeddings still returns sensible
    results. Stop words are ignored and repeats are damped (1 + log count).
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
        tokens = Counter(t for t in _TOKEN.findall(normalize(text).lower()) if t not in STOP_WORDS)
        for token, count in tokens.items():
            digest = hashlib.sha256(token.encode()).digest()
            index = int.from_bytes(digest[:8], "big") % self._dimension
            weight = 1.0 + math.log(count)
            vector[index] += weight if digest[8] & 1 else -weight
        if not any(vector):
            # No word tokens (or they cancelled out): fall back to a vector
            # seeded by the whole text so it's still non-zero and unique.
            seed = hashlib.sha256(normalize(text).encode()).digest()
            rng = random.Random(seed)
            vector = [rng.gauss(0.0, 1.0) for _ in range(self._dimension)]
        norm = math.sqrt(sum(v * v for v in vector))
        return [v / norm for v in vector]
