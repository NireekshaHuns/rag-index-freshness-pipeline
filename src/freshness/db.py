"""Connection pool and a minimal SQL migration runner."""

import logging
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg_pool import ConnectionPool

from freshness.config import Settings

log = logging.getLogger(__name__)

DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"

# Arbitrary constant; serializes migration runs when several services start at once.
MIGRATION_LOCK_ID = 7_428_331

# Placeholders substituted into migration SQL from settings.
EMBEDDING_DIMENSION_PLACEHOLDER = "{{EMBEDDING_DIMENSION}}"


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path


def create_pool(settings: Settings, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    pool = ConnectionPool(settings.database_url, min_size=min_size, max_size=max_size, open=False)
    pool.open(wait=True)
    return pool


def discover_migrations(directory: Path) -> list[Migration]:
    migrations = []
    for path in sorted(directory.glob("*.sql")):
        prefix, _, _ = path.stem.partition("_")
        if not prefix.isdigit():
            raise ValueError(f"migration file must start with a number: {path.name}")
        migrations.append(Migration(version=int(prefix), name=path.stem, path=path))
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise ValueError(f"duplicate migration numbers in {directory}")
    return migrations


def render(sql: str, settings: Settings) -> str:
    return sql.replace(EMBEDDING_DIMENSION_PLACEHOLDER, str(settings.embedding_dimension))


def migrate(
    database_url: str, settings: Settings, directory: Path = DEFAULT_MIGRATIONS_DIR
) -> list[str]:
    """Apply pending migrations, each in its own transaction. Returns names applied."""
    migrations = discover_migrations(directory)
    applied_now: list[str] = []
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INT PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for migration in migrations:
                if migration.version in done:
                    continue
                sql = render(migration.path.read_text(), settings)
                with conn.transaction():
                    conn.execute(sql)  # type: ignore[call-overload]
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                        (migration.version, migration.name),
                    )
                log.info("applied migration %s", migration.name)
                applied_now.append(migration.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
    return applied_now
