import math
import random

import pytest

from freshness.config import Settings
from freshness.embeddings import (
    TransientEmbeddingError,
    create_provider,
)
from freshness.embeddings.failing import FailureInjectingProvider
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.embeddings.openai import OpenAIEmbeddingProvider


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


class TestFakeProvider:
    provider = FakeEmbeddingProvider(dimension=64)

    def test_vectors_have_configured_dimension_and_unit_length(self) -> None:
        [vector] = self.provider.embed(["The quick brown fox."])
        assert len(vector) == 64
        assert math.isclose(math.sqrt(sum(v * v for v in vector)), 1.0)

    def test_is_deterministic_across_instances(self) -> None:
        text = "Refund policy: 30 days."
        assert self.provider.embed([text]) == FakeEmbeddingProvider(64).embed([text])

    def test_whitespace_and_case_do_not_matter(self) -> None:
        a, b = self.provider.embed(["Refund policy", "  refund\nPOLICY "])
        assert a == b

    def test_shared_words_are_closer_than_unrelated_text(self) -> None:
        query, related, unrelated = self.provider.embed(
            [
                "how long is the refund window",
                "The refund window is 30 days from purchase.",
                "Our office kitchen is cleaned every Friday.",
            ]
        )
        assert cosine(query, related) > cosine(query, unrelated)

    def test_text_without_words_still_gets_a_unit_vector(self) -> None:
        a, b = self.provider.embed(["!!!", "???"])
        assert math.isclose(sum(v * v for v in a), 1.0)
        assert a != b

    def test_preserves_input_order(self) -> None:
        texts = [f"text number {i}" for i in range(5)]
        assert self.provider.embed(texts) == [self.provider.embed([t])[0] for t in texts]


class TestFailureInjection:
    inner = FakeEmbeddingProvider(dimension=8)

    def test_rate_zero_never_fails(self) -> None:
        provider = FailureInjectingProvider(self.inner, 0.0)
        for _ in range(100):
            provider.embed(["ok"])

    def test_rate_one_always_fails(self) -> None:
        provider = FailureInjectingProvider(self.inner, 1.0)
        for _ in range(20):
            with pytest.raises(TransientEmbeddingError):
                provider.embed(["ok"])

    def test_partial_rate_fails_roughly_that_often(self) -> None:
        provider = FailureInjectingProvider(self.inner, 0.3, rng=random.Random(42))
        failures = 0
        for _ in range(2000):
            try:
                provider.embed(["ok"])
            except TransientEmbeddingError:
                failures += 1
        assert 0.25 < failures / 2000 < 0.35

    def test_delegates_on_success(self) -> None:
        provider = FailureInjectingProvider(self.inner, 0.0)
        assert provider.dimension == 8
        assert provider.embed(["x"]) == self.inner.embed(["x"])

    def test_rejects_invalid_rate(self) -> None:
        with pytest.raises(ValueError):
            FailureInjectingProvider(self.inner, 1.5)


class TestFactory:
    def test_defaults_to_fake(self) -> None:
        provider = create_provider(Settings(embedding_dimension=16))
        assert isinstance(provider, FakeEmbeddingProvider)
        assert provider.dimension == 16

    def test_wraps_with_failure_injection_when_rate_set(self) -> None:
        provider = create_provider(Settings(embedding_failure_rate=0.5))
        assert isinstance(provider, FailureInjectingProvider)

    def test_builds_openai_provider_without_calling_the_api(self) -> None:
        settings = Settings(embedding_provider="openai", openai_api_key="sk-test")
        assert isinstance(create_provider(settings), OpenAIEmbeddingProvider)
