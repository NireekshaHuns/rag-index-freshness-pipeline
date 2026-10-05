import socket
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from freshness.config import Settings
from freshness.db import migrate


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


@pytest.fixture
def database_url(empty_database_url: str) -> str:
    """A fresh database with all migrations applied."""
    migrate(empty_database_url, Settings(database_url=empty_database_url))
    return empty_database_url


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """Single-node KRaft broker using the same image as docker-compose.

    The host port is fixed up front so the advertised listener matches what
    clients on the host actually connect to.
    """
    port = free_port()
    container = (
        DockerContainer("apache/kafka:3.9.1")
        .with_bind_ports(9092, port)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://localhost:{port}")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(120))
    )
    with container:
        yield f"localhost:{port}"


@pytest.fixture
def kafka_topic(kafka_bootstrap: str) -> str:
    """A fresh multi-partition topic per test."""
    name = f"document-changes-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    for future in admin.create_topics([NewTopic(name, num_partitions=4)]).values():
        future.result(timeout=30)
    return name
