import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from psycopg_pool import ConnectionPool

from freshness.api import store
from freshness.config import Settings
from freshness.db import create_pool
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.hashing import content_hash
from freshness.indexer.processor import DocumentIndexer
from freshness.vectors import to_pgvector
from freshness.verify import VerifyReport, verify

DIMENSION = Settings().embedding_dimension
PROVIDER = FakeEmbeddingProvider(DIMENSION)
SCRIPT = Path(__file__).parents[2] / "scripts" / "verify.py"


def text(*labels: str) -> str:
    return "\n\n".join(" ".join(f"{label} sentence {i}." for i in range(8)) for label in labels)


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = create_pool(Settings(database_url=database_url), max_size=2)
    yield pool
    pool.close()


@pytest.fixture
def docs(pool: ConnectionPool) -> dict[str, uuid.UUID]:
    """A consistent index: two live documents and one deleted."""
    indexer = DocumentIndexer(pool, PROVIDER)
    with pool.connection() as conn:
        live = store.create_document(conn, "Live", text("a", "b", "c")).id
        other = store.create_document(conn, "Other", text("x", "y")).id
        gone = store.create_document(conn, "Gone", text("g")).id
        for doc_id in (live, other, gone):
            indexer.process(doc_id)
        store.delete_document(conn, gone)
    indexer.process(gone)
    return {"live": live, "other": other, "gone": gone}


def run(pool: ConnectionPool) -> VerifyReport:
    with pool.connection() as conn:
        return verify(conn, embeddings=PROVIDER)


def sql(pool: ConnectionPool, statement: str, params: tuple[Any, ...] = ()) -> None:
    with pool.connection() as conn:
        conn.execute(statement, params)


def kinds(report: VerifyReport) -> set[str]:
    return {v.kind for v in report.violations}


def test_consistent_index_passes(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    report = run(pool)
    assert report.ok, report.violations
    assert (report.live_documents, report.deleted_documents, report.chunks) == (2, 1, 5)


def test_unindexed_edit_is_a_version_mismatch(
    pool: ConnectionPool, docs: dict[str, uuid.UUID]
) -> None:
    with pool.connection() as conn:
        store.update_document(conn, docs["live"], "Live", text("a", "b edited", "c"))

    report = run(pool)

    assert kinds(report) == {"version_mismatch", "missing_chunks", "stale_chunks"}
    assert {v.document_id for v in report.violations} == {docs["live"]}


def test_never_indexed_document(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    with pool.connection() as conn:
        store.create_document(conn, "New", text("n"))
    assert kinds(run(pool)) == {"missing_index_state", "missing_chunks"}


def test_leftover_chunks_of_a_removed_paragraph(
    pool: ConnectionPool, docs: dict[str, uuid.UUID]
) -> None:
    vector = to_pgvector(PROVIDER.embed(["stale"])[0])
    sql(
        pool,
        """
        INSERT INTO chunks (document_id, content_hash, position, content, embedding,
                            document_version)
        VALUES (%s, %s, 9, 'stale', %s::vector, 1)
        """,
        (docs["live"], content_hash("stale"), vector),
    )
    assert kinds(run(pool)) == {"stale_chunks"}


def test_deleted_document_with_chunks(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(
        pool,
        "UPDATE chunks SET document_id = %s WHERE document_id = %s",
        (docs["gone"], docs["other"]),
    )
    assert {"deleted_has_chunks", "missing_chunks"} <= kinds(run(pool))


def test_delete_not_recorded(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(pool, "UPDATE index_state SET deleted = false WHERE document_id = %s", (docs["gone"],))
    assert kinds(run(pool)) == {"delete_not_recorded"}


def test_live_document_marked_deleted(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(pool, "UPDATE index_state SET deleted = true WHERE document_id = %s", (docs["live"],))
    assert kinds(run(pool)) == {"wrongly_deleted"}


def test_orphan_chunks(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(
        pool,
        "UPDATE chunks SET document_id = %s WHERE document_id = %s",
        (uuid.uuid4(), docs["other"]),
    )
    assert kinds(run(pool)) == {"orphan_chunk", "missing_chunks"}


def test_wrong_positions_and_duplicates(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(pool, "UPDATE chunks SET position = 0 WHERE document_id = %s", (docs["live"],))
    assert kinds(run(pool)) == {"duplicate_position", "position_mismatch"}


def test_corrupted_chunk_content(pool: ConnectionPool, docs: dict[str, uuid.UUID]) -> None:
    sql(
        pool,
        """
        UPDATE chunks SET content = content || ' tampered'
        WHERE document_id = %s AND position = 0
        """,
        (docs["live"],),
    )
    assert "hash_mismatch" in kinds(run(pool))


def test_embedding_reused_from_the_wrong_text(
    pool: ConnectionPool, docs: dict[str, uuid.UUID]
) -> None:
    sql(
        pool,
        """
        UPDATE chunks SET embedding = (
            SELECT embedding FROM chunks WHERE document_id = %s LIMIT 1
        ) WHERE document_id = %s
        """,
        (docs["other"], docs["live"]),
    )
    assert kinds(run(pool)) == {"embedding_mismatch"}


def run_script(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DATABASE_URL": database_url}
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60
    )


def test_script_exit_codes(
    database_url: str, pool: ConnectionPool, docs: dict[str, uuid.UUID]
) -> None:
    ok = run_script(database_url, "--embeddings")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "OK: index is consistent" in ok.stdout

    sql(pool, "DELETE FROM index_state WHERE document_id = %s", (docs["live"],))
    failed = run_script(database_url)
    assert failed.returncode == 1
    assert "missing_index_state: 1" in failed.stdout
