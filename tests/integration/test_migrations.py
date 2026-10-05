from pathlib import Path

import psycopg
import pytest

from freshness.config import Settings
from freshness.db import create_pool, migrate

SETTINGS = Settings(embedding_dimension=16)
EXPECTED_TABLES = {"documents", "outbox", "chunks", "index_state", "schema_migrations"}


def tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ).fetchall()
    return {row[0] for row in rows}


def test_applies_full_schema(empty_database_url: str) -> None:
    applied = migrate(empty_database_url, SETTINGS)

    assert applied == ["001_init"]
    assert tables(empty_database_url) == EXPECTED_TABLES
    with psycopg.connect(empty_database_url) as conn:
        column_type = conn.execute(
            """
            SELECT format_type(atttypid, atttypmod) FROM pg_attribute
            WHERE attrelid = 'chunks'::regclass AND attname = 'embedding'
            """
        ).fetchone()
        index_def = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'chunks_embedding_hnsw_idx'"
        ).fetchone()
    assert column_type == ("vector(16)",)
    assert index_def is not None and "hnsw" in index_def[0]


def test_rerun_applies_nothing(empty_database_url: str) -> None:
    migrate(empty_database_url, SETTINGS)
    assert migrate(empty_database_url, SETTINGS) == []


def test_chunk_hash_is_unique_per_document(empty_database_url: str) -> None:
    migrate(empty_database_url, SETTINGS)
    vector = "[" + ",".join(["0.1"] * 16) + "]"
    insert = """
        INSERT INTO chunks (document_id, content_hash, position, content, embedding,
                            document_version)
        VALUES ('00000000-0000-0000-0000-000000000001', 'h', %s, 'c', %s, 1)
    """
    with psycopg.connect(empty_database_url) as conn:
        conn.execute(insert, (0, vector))
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(insert, (1, vector))


def test_failed_migration_is_rolled_back(empty_database_url: str, tmp_path: Path) -> None:
    (tmp_path / "001_good.sql").write_text("CREATE TABLE good (id INT);")
    (tmp_path / "002_bad.sql").write_text("CREATE TABLE partial (id INT); SELECT nope;")

    with pytest.raises(psycopg.errors.UndefinedColumn):
        migrate(empty_database_url, SETTINGS, tmp_path)

    assert tables(empty_database_url) == {"good", "schema_migrations"}
    with psycopg.connect(empty_database_url) as conn:
        versions = conn.execute("SELECT version FROM schema_migrations").fetchall()
    assert versions == [(1,)]


def test_pool_connects(empty_database_url: str) -> None:
    pool = create_pool(Settings(database_url=empty_database_url), max_size=2)
    try:
        with pool.connection() as conn:
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        pool.close()
