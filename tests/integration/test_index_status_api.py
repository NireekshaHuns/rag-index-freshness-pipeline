import uuid
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from freshness.api.app import create_app
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.indexer.processor import DocumentIndexer

DIMENSION = Settings().embedding_dimension

# Each paragraph is long enough to be its own chunk.
POLICY = """Employees receive twenty days of paid vacation each year, accrued monthly from \
their start date.

Unused vacation days roll over up to a maximum of five days into the next calendar year.

Sick leave is unlimited but requires a doctor's note after three consecutive days away."""


@pytest.fixture
def client(database_url: str) -> Iterator[TestClient]:
    app = create_app(Settings(database_url=database_url))
    with TestClient(app) as client:
        yield client


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=2)
    yield pool
    pool.close()


@pytest.fixture
def index(pool: ConnectionPool) -> DocumentIndexer:
    return DocumentIndexer(pool, FakeEmbeddingProvider(DIMENSION))


def mark_published(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute("UPDATE outbox SET published_at = clock_timestamp()")


def status(client: TestClient, doc_id: str) -> dict:
    response = client.get(f"/documents/{doc_id}/index")
    assert response.status_code == 200, response.text
    return response.json()


def test_before_indexing_nothing_is_indexed(client: TestClient) -> None:
    doc = client.post("/documents", json={"title": "Policy", "content": POLICY}).json()

    body = status(client, doc["id"])

    assert body["version"] == 1
    assert body["published_at"] is None
    assert body["indexed_version"] is None
    assert body["indexed_at"] is None
    assert body["chunks"] == []


def test_reports_timestamps_in_pipeline_order(
    client: TestClient, pool: ConnectionPool, index: DocumentIndexer
) -> None:
    doc = client.post("/documents", json={"title": "Policy", "content": POLICY}).json()
    mark_published(pool)
    index.process(uuid.UUID(doc["id"]))

    body = status(client, doc["id"])

    assert body["indexed_version"] == 1
    assert body["updated_at"] <= body["published_at"] <= body["indexed_at"]
    assert [c["position"] for c in body["chunks"]] == [0, 1, 2]
    assert all(c["embedded_at_version"] == 1 for c in body["chunks"])


def test_shows_which_chunks_an_edit_re_embedded(client: TestClient, index: DocumentIndexer) -> None:
    doc = client.post("/documents", json={"title": "Policy", "content": POLICY}).json()
    doc_id = doc["id"]
    index.process(uuid.UUID(doc_id))
    edited = POLICY.replace("maximum of five days", "maximum of ten days")
    client.put(f"/documents/{doc_id}", json={"title": "Policy", "content": edited})
    index.process(uuid.UUID(doc_id))

    chunks = status(client, doc_id)["chunks"]

    assert [c["embedded_at_version"] for c in chunks] == [1, 2, 1]
    assert "ten days" in chunks[1]["content"]


def test_published_at_belongs_to_the_current_version(
    client: TestClient, pool: ConnectionPool
) -> None:
    doc = client.post("/documents", json={"title": "Policy", "content": POLICY}).json()
    mark_published(pool)
    client.put(f"/documents/{doc['id']}", json={"title": "Policy", "content": POLICY + " More."})

    body = status(client, doc["id"])

    assert body["version"] == 2
    assert body["published_at"] is None


def test_follows_a_delete_through_the_index(client: TestClient, index: DocumentIndexer) -> None:
    doc = client.post("/documents", json={"title": "Policy", "content": POLICY}).json()
    index.process(uuid.UUID(doc["id"]))
    client.delete(f"/documents/{doc['id']}")
    index.process(uuid.UUID(doc["id"]))

    body = status(client, doc["id"])

    assert body["deleted"] is True
    assert body["indexed_version"] == 2
    assert body["chunks"] == []


def test_unknown_document_is_not_found(client: TestClient) -> None:
    assert client.get(f"/documents/{uuid.uuid4()}/index").status_code == 404
