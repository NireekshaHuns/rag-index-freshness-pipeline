import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from harness import Pipeline
from psycopg_pool import ConnectionPool

from freshness.config import Settings
from freshness.db import create_pool


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/chaos" in str(item.path):
            item.add_marker(pytest.mark.chaos)


@pytest.fixture
def settings(database_url: str, kafka_bootstrap: str) -> Settings:
    """A fresh database, topic, DLQ, and consumer group for each scenario."""
    suffix = uuid.uuid4().hex[:8]
    topic, dlq = f"chaos-{suffix}", f"chaos-{suffix}.dlq"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    futures = admin.create_topics([NewTopic(topic, num_partitions=6), NewTopic(dlq, 1)])
    for future in futures.values():
        future.result(timeout=30)
    return Settings(
        database_url=database_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        kafka_topic=topic,
        kafka_dlq_topic=dlq,
        kafka_consumer_group=f"indexer-{suffix}",
    )


@pytest.fixture
def pool(settings: Settings) -> Iterator[ConnectionPool]:
    pool = create_pool(settings, max_size=4)
    yield pool
    pool.close()


@pytest.fixture
def pipeline(settings: Settings, tmp_path: Path) -> Iterator[Pipeline]:
    pipeline = Pipeline(settings, tmp_path)
    yield pipeline
    crashed = pipeline.crashed()
    pipeline.stop_all()
    assert not crashed, f"services died during the scenario: {crashed}\n{pipeline.logs()}"
