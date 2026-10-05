"""Exercises the real OpenAI SDK against a mocked HTTP transport: no key, no network."""

import json
from collections.abc import Callable

import httpx2
import openai
import pytest

from freshness.embeddings import PermanentEmbeddingError, TransientEmbeddingError
from freshness.embeddings.openai import MODEL, OpenAIEmbeddingProvider

Handler = Callable[[httpx2.Request], httpx2.Response]


def provider(handler: Handler, dimension: int = 4) -> OpenAIEmbeddingProvider:
    client = openai.OpenAI(
        api_key="sk-test",
        max_retries=0,
        http_client=httpx2.Client(transport=httpx2.MockTransport(handler)),
    )
    return OpenAIEmbeddingProvider("sk-test", dimension, client=client)


def embeddings_response(vectors: list[list[float]], reverse: bool = False) -> httpx2.Response:
    data = [{"object": "embedding", "index": i, "embedding": v} for i, v in enumerate(vectors)]
    if reverse:
        data.reverse()
    body = {
        "object": "list",
        "data": data,
        "model": MODEL,
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }
    return httpx2.Response(200, json=body)


def test_sends_model_and_dimension_and_returns_vectors_in_order() -> None:
    requests: list[dict[str, object]] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(json.loads(request.content))
        return embeddings_response([[0.1] * 4, [0.2] * 4], reverse=True)

    vectors = provider(handler).embed(["a", "b"])

    assert vectors == [[0.1] * 4, [0.2] * 4]
    assert requests[0]["model"] == MODEL
    assert requests[0]["dimensions"] == 4
    assert requests[0]["input"] == ["a", "b"]


def test_empty_input_makes_no_request() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise AssertionError("no request expected")

    assert provider(handler).embed([]) == []


@pytest.mark.parametrize("status", [429, 500, 503])
def test_retryable_statuses_are_transient(status: int) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(status, json={"error": {"message": "try later"}})

    with pytest.raises(TransientEmbeddingError):
        provider(handler).embed(["a"])


def test_connection_errors_are_transient() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("refused", request=request)

    with pytest.raises(TransientEmbeddingError):
        provider(handler).embed(["a"])


@pytest.mark.parametrize("status", [400, 401, 403])
def test_client_errors_are_permanent(status: int) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(status, json={"error": {"message": "nope"}})

    with pytest.raises(PermanentEmbeddingError):
        provider(handler).embed(["a"])


def test_wrong_dimension_is_rejected() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return embeddings_response([[0.1] * 3])

    with pytest.raises(PermanentEmbeddingError, match="dimension"):
        provider(handler).embed(["a"])
