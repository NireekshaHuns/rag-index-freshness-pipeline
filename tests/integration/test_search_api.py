import uuid
from collections.abc import Iterator, Sequence

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from freshness.api.app import create_app
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings import TransientEmbeddingError
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.indexer.processor import DocumentIndexer

DIMENSION = Settings().embedding_dimension

HANDBOOK = """# Leave policy

Employees receive twenty days of paid vacation each year, accrued monthly.

Unused vacation days roll over up to a maximum of five days into the next year.

Sick leave is unlimited but requires a doctor's note after three consecutive days."""


class SwitchableProvider(FakeEmbeddingProvider):
    def __init__(self) -> None:
        super().__init__(DIMENSION)
        self.failing = False

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if self.failing:
            raise TransientEmbeddingError("provider down")
        return super().embed(texts)


@pytest.fixture
def provider() -> SwitchableProvider:
    return SwitchableProvider()


@pytest.fixture
def client(database_url: str, provider: SwitchableProvider) -> Iterator[TestClient]:
    app = create_app(Settings(database_url=database_url), embedding_provider=provider)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=2)
    yield pool
    pool.close()


@pytest.fixture
def index(pool: ConnectionPool) -> DocumentIndexer:
    return DocumentIndexer(pool, FakeEmbeddingProvider(DIMENSION))


def search(
    client: TestClient, query: str, top_k: int = 3, document_id: str | None = None
) -> list[dict[str, object]]:
    body: dict[str, object] = {"query": query, "top_k": top_k}
    if document_id:
        body["document_id"] = document_id
    response = client.post("/search", json=body)
    assert response.status_code == 200, response.text
    return response.json()["results"]


def test_finds_the_most_relevant_chunk(client: TestClient, index: DocumentIndexer) -> None:
    doc = client.post("/documents", json={"title": "Handbook", "content": HANDBOOK}).json()
    index.process(uuid.UUID(doc["id"]))

    [top, *_] = search(client, "how many vacation days roll over")

    assert "roll over up to a maximum of five days" in top["content"]
    assert top["title"] == "Handbook"
    assert top["document_version"] == 1
    assert top["document_id"] == doc["id"]


def test_results_are_ordered_by_score_and_limited(
    client: TestClient, index: DocumentIndexer
) -> None:
    doc = client.post("/documents", json={"title": "Handbook", "content": HANDBOOK}).json()
    index.process(uuid.UUID(doc["id"]))

    results = search(client, "sick leave doctor's note", top_k=2)

    assert len(results) == 2
    scores = [r["score"] for r in results]
    assert scores == sorted(scores, reverse=True)
    assert "Sick leave" in results[0]["content"]


def test_results_reflect_an_edit_after_reindex(client: TestClient, index: DocumentIndexer) -> None:
    doc = client.post("/documents", json={"title": "Handbook", "content": HANDBOOK}).json()
    doc_id = uuid.UUID(doc["id"])
    index.process(doc_id)
    edited = HANDBOOK.replace("twenty days of paid vacation", "twenty five days of paid vacation")
    client.put(f"/documents/{doc_id}", json={"title": "Handbook", "content": edited})

    # Until the indexer catches up, search still serves the indexed version.
    [stale] = search(client, "paid vacation each year", top_k=1)
    assert "twenty days" in stale["content"] and stale["document_version"] == 1

    index.process(doc_id)

    [fresh] = search(client, "paid vacation each year", top_k=1)
    assert "twenty five days" in fresh["content"]
    assert fresh["document_version"] == 2
    all_contents = [r["content"] for r in search(client, "paid vacation", top_k=10)]
    assert not any("twenty days of paid" in c for c in all_contents)


def test_deleted_documents_disappear_from_results(
    client: TestClient, index: DocumentIndexer
) -> None:
    doc = client.post("/documents", json={"title": "Handbook", "content": HANDBOOK}).json()
    doc_id = uuid.UUID(doc["id"])
    index.process(doc_id)
    client.delete(f"/documents/{doc_id}")

    # Hidden immediately, even before the indexer removes the chunks.
    assert search(client, "vacation") == []
    index.process(doc_id)
    assert search(client, "vacation") == []


def test_empty_index_returns_no_results(client: TestClient) -> None:
    assert search(client, "anything") == []


def test_invalid_requests_are_rejected(client: TestClient) -> None:
    assert client.post("/search", json={"query": ""}).status_code == 422
    assert client.post("/search", json={"query": "x", "top_k": 0}).status_code == 422
    assert client.post("/search", json={"query": "x", "top_k": 51}).status_code == 422


def test_embedding_outage_returns_503(client: TestClient, provider: SwitchableProvider) -> None:
    provider.failing = True
    assert client.post("/search", json={"query": "vacation"}).status_code == 503


def test_search_can_be_scoped_to_one_document(client: TestClient, index: DocumentIndexer) -> None:
    handbook = client.post("/documents", json={"title": "Handbook", "content": HANDBOOK}).json()
    other_content = HANDBOOK.replace("Leave policy", "Leave policy for contractors")
    other = client.post("/documents", json={"title": "Other", "content": other_content}).json()
    for doc in (handbook, other):
        index.process(uuid.UUID(doc["id"]))

    results = search(client, "vacation days", top_k=10, document_id=other["id"])

    assert results and {r["document_id"] for r in results} == {other["id"]}
    assert search(client, "vacation", document_id=str(uuid.uuid4())) == []


def test_deleted_documents_do_not_starve_top_k(client: TestClient, index: DocumentIndexer) -> None:
    """Many deleted near-duplicates must not crowd out the live result."""
    for i in range(60):
        doc = client.post(
            "/documents", json={"title": f"Old {i}", "content": f"Paid vacation days {i}."}
        ).json()
        index.process(uuid.UUID(doc["id"]))
        client.delete(f"/documents/{doc['id']}")  # chunks stay until reindexed
    live = client.post("/documents", json={"title": "Live", "content": HANDBOOK}).json()
    index.process(uuid.UUID(live["id"]))

    results = search(client, "paid vacation days", top_k=3)

    assert len(results) == 3
    assert {r["document_id"] for r in results} == {live["id"]}
