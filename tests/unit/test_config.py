import pytest

from freshness.config import ConfigError, Settings


def test_defaults_without_env() -> None:
    settings = Settings.from_env({})
    assert settings.embedding_provider == "fake"
    assert settings.kafka_topic == "document-changes"
    assert settings.embedding_failure_rate == 0.0


def test_reads_and_parses_env() -> None:
    settings = Settings.from_env(
        {
            "DATABASE_URL": "postgresql://u:p@db:5432/x",
            "EMBEDDING_DIMENSION": "1536",
            "EMBEDDING_FAILURE_RATE": "0.3",
            "MAX_RETRIES": "2",
            "RELAY_POLL_INTERVAL_MS": "50",
        }
    )
    assert settings.database_url == "postgresql://u:p@db:5432/x"
    assert settings.embedding_dimension == 1536
    assert settings.embedding_failure_rate == 0.3
    assert settings.max_retries == 2
    assert settings.relay_poll_interval_ms == 50


def test_blank_values_fall_back_to_defaults() -> None:
    assert Settings.from_env({"MAX_RETRIES": "  "}).max_retries == Settings().max_retries


def test_openai_requires_api_key() -> None:
    with pytest.raises(ConfigError, match="OPENAI_API_KEY"):
        Settings.from_env({"EMBEDDING_PROVIDER": "openai"})
    settings = Settings.from_env({"EMBEDDING_PROVIDER": "OpenAI", "OPENAI_API_KEY": "sk-test"})
    assert settings.embedding_provider == "openai"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("EMBEDDING_PROVIDER", "cohere"),
        ("EMBEDDING_FAILURE_RATE", "1.5"),
        ("EMBEDDING_DIMENSION", "0"),
        ("MAX_RETRIES", "-1"),
        ("MAX_RETRIES", "three"),
        ("RELAY_POLL_INTERVAL_MS", "0"),
    ],
)
def test_rejects_invalid_values(name: str, value: str) -> None:
    with pytest.raises(ConfigError):
        Settings.from_env({name: value})
