import uuid
from collections.abc import Iterator

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration" in str(item.path):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer("pgvector/pgvector:pg16", driver=None) as container:
        yield container


@pytest.fixture
def empty_database_url(postgres_container: PostgresContainer) -> str:
    """A brand-new database per test, so tests never see each other's schema."""
    admin_url = postgres_container.get_connection_url()
    name = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    return admin_url.rsplit("/", 1)[0] + f"/{name}"
