import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient

from freshness.api import store
from freshness.api.app import create_app
from freshness.config import Settings
from freshness.db import create_pool


@pytest.fixture
def client(database_url: str) -> Iterator[TestClient]:
    app = create_app(Settings(database_url=database_url))
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


def query(url: str, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(url) as conn:
        return conn.execute(sql, params).fetchall()


def outbox_rows(url: str) -> list[tuple[Any, ...]]:
    return query(
        url,
        "SELECT document_id, event_type, document_version, published_at FROM outbox ORDER BY id",
    )


def put(client: TestClient, doc_id: object, content: str = "x") -> int:
    return client.put(f"/documents/{doc_id}", json={"title": "T", "content": content}).status_code


def break_outbox(url: str) -> None:
    """Make every outbox insert fail, simulating a crash mid-transaction."""
    with psycopg.connect(url) as conn:
        conn.execute(
            """
            CREATE FUNCTION fail_outbox() RETURNS trigger AS $$
            BEGIN RAISE EXCEPTION 'outbox unavailable'; END $$ LANGUAGE plpgsql;
            CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION fail_outbox();
            """
        )


def test_create_writes_document_and_outbox_together(client: TestClient, database_url: str) -> None:
    response = client.post("/documents", json={"title": "Handbook", "content": "Hello."})

    assert response.status_code == 201
    doc = response.json()
    assert doc["version"] == 1
    doc_id = uuid.UUID(doc["id"])
    assert outbox_rows(database_url) == [(doc_id, "upserted", 1, None)]
    # Same transaction, so the change timestamp and event timestamp agree exactly.
    [(updated_at, created_at)] = query(
        database_url,
        "SELECT d.updated_at, o.created_at FROM documents d JOIN outbox o ON o.document_id = d.id",
    )
    assert updated_at == created_at


def test_get_returns_document(client: TestClient) -> None:
    created = client.post("/documents", json={"title": "T", "content": "C"}).json()

    response = client.get(f"/documents/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_update_increments_version_and_enqueues(client: TestClient, database_url: str) -> None:
    doc_id = client.post("/documents", json={"title": "T", "content": "v1"}).json()["id"]

    response = client.put(f"/documents/{doc_id}", json={"title": "T2", "content": "v2"})

    assert response.status_code == 200
    assert response.json()["version"] == 2
    assert response.json()["content"] == "v2"
    rows = outbox_rows(database_url)
    assert [(r[1], r[2]) for r in rows] == [("upserted", 1), ("upserted", 2)]


def test_delete_bumps_version_and_hides_document(client: TestClient, database_url: str) -> None:
    doc_id = client.post("/documents", json={"title": "T", "content": "C"}).json()["id"]

    response = client.delete(f"/documents/{doc_id}")

    assert response.status_code == 200
    assert response.json() == {"id": doc_id, "version": 2, "deleted": True}
    assert client.get(f"/documents/{doc_id}").status_code == 404
    assert put(client, doc_id) == 404
    assert client.delete(f"/documents/{doc_id}").status_code == 404
    rows = outbox_rows(database_url)
    assert [(r[1], r[2]) for r in rows] == [("upserted", 1), ("deleted", 2)]


def test_unknown_document_is_404_and_enqueues_nothing(
    client: TestClient, database_url: str
) -> None:
    missing = uuid.uuid4()
    assert client.get(f"/documents/{missing}").status_code == 404
    assert put(client, missing) == 404
    assert client.delete(f"/documents/{missing}").status_code == 404
    assert outbox_rows(database_url) == []


def test_invalid_body_is_rejected(client: TestClient, database_url: str) -> None:
    assert client.post("/documents", json={"title": "", "content": "C"}).status_code == 422
    assert client.post("/documents", json={"title": "T"}).status_code == 422
    assert query(database_url, "SELECT count(*) FROM documents") == [(0,)]


def test_failed_outbox_insert_rolls_back_create(client: TestClient, database_url: str) -> None:
    break_outbox(database_url)

    response = client.post("/documents", json={"title": "T", "content": "C"})

    assert response.status_code == 500
    assert query(database_url, "SELECT count(*) FROM documents") == [(0,)]
    assert outbox_rows(database_url) == []


def test_failed_outbox_insert_rolls_back_update_and_delete(
    client: TestClient, database_url: str
) -> None:
    doc_id = client.post("/documents", json={"title": "T", "content": "v1"}).json()["id"]
    break_outbox(database_url)

    assert put(client, doc_id, "v2") == 500
    assert client.delete(f"/documents/{doc_id}").status_code == 500

    assert query(database_url, "SELECT content, version, deleted FROM documents") == [
        ("v1", 1, False)
    ]
    assert len(outbox_rows(database_url)) == 1


def test_concurrent_updates_get_distinct_versions(database_url: str) -> None:
    pool = create_pool(Settings(database_url=database_url), max_size=8)
    try:
        with pool.connection() as conn:
            doc = store.create_document(conn, "T", "v0")

        def edit(i: int) -> int:
            with pool.connection() as conn:
                updated = store.update_document(conn, doc.id, "T", f"v{i}")
                assert updated is not None
                return updated.version

        with ThreadPoolExecutor(max_workers=8) as executor:
            versions = sorted(executor.map(edit, range(20)))
    finally:
        pool.close()

    assert versions == list(range(2, 22))
    outbox_versions = [r[2] for r in outbox_rows(database_url)]
    assert sorted(outbox_versions) == list(range(1, 22))
