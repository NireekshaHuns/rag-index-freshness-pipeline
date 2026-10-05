"""Settings loaded from environment variables."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

EmbeddingProviderName = Literal["fake", "openai"]


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://freshness:freshness@localhost:5433/freshness"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "document-changes"
    kafka_dlq_topic: str = "document-changes.dlq"
    kafka_consumer_group: str = "indexer"
    # How long a crashed consumer keeps its partitions before they're reassigned.
    kafka_session_timeout_ms: int = 10_000
    embedding_provider: EmbeddingProviderName = "fake"
    openai_api_key: str | None = None
    embedding_dimension: int = 384
    embedding_failure_rate: float = 0.0
    max_retries: int = 5
    retry_base_delay_seconds: float = 0.5
    reconcile_interval_seconds: float = 300.0
    # Changes younger than this are left to the event path.
    reconcile_grace_seconds: float = 60.0
    relay_poll_interval_ms: int = 200
    metrics_port: int = 9100

    def __post_init__(self) -> None:
        if self.embedding_provider not in ("fake", "openai"):
            raise ConfigError(
                f"EMBEDDING_PROVIDER must be 'fake' or 'openai', got {self.embedding_provider!r}"
            )
        if self.embedding_provider == "openai" and not self.openai_api_key:
            raise ConfigError("OPENAI_API_KEY is required when EMBEDDING_PROVIDER=openai")
        if self.embedding_dimension <= 0:
            raise ConfigError("EMBEDDING_DIMENSION must be positive")
        if not 0.0 <= self.embedding_failure_rate <= 1.0:
            raise ConfigError("EMBEDDING_FAILURE_RATE must be between 0 and 1")
        if self.max_retries < 0:
            raise ConfigError("MAX_RETRIES must not be negative")
        if self.retry_base_delay_seconds < 0:
            raise ConfigError("RETRY_BASE_DELAY_SECONDS must not be negative")
        if self.reconcile_interval_seconds <= 0:
            raise ConfigError("RECONCILE_INTERVAL_SECONDS must be positive")
        if self.reconcile_grace_seconds < 0:
            raise ConfigError("RECONCILE_GRACE_SECONDS must not be negative")
        if self.kafka_session_timeout_ms <= 0:
            raise ConfigError("KAFKA_SESSION_TIMEOUT_MS must be positive")
        if self.relay_poll_interval_ms <= 0:
            raise ConfigError("RELAY_POLL_INTERVAL_MS must be positive")
        if not 0 < self.metrics_port < 65536:
            raise ConfigError("METRICS_PORT must be a valid port")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        defaults = cls()

        def get(name: str, default: object) -> str:
            # Empty values fall back to defaults so a blank line in .env is harmless.
            value = env.get(name, "").strip()
            return value if value else str(default)

        try:
            return cls(
                database_url=get("DATABASE_URL", defaults.database_url),
                kafka_bootstrap_servers=get(
                    "KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
                ),
                kafka_topic=get("KAFKA_TOPIC", defaults.kafka_topic),
                kafka_dlq_topic=get("KAFKA_DLQ_TOPIC", defaults.kafka_dlq_topic),
                kafka_consumer_group=get("KAFKA_CONSUMER_GROUP", defaults.kafka_consumer_group),
                kafka_session_timeout_ms=int(
                    get("KAFKA_SESSION_TIMEOUT_MS", defaults.kafka_session_timeout_ms)
                ),
                embedding_provider=get("EMBEDDING_PROVIDER", defaults.embedding_provider).lower(),  # type: ignore[arg-type]
                openai_api_key=env.get("OPENAI_API_KEY", "").strip() or None,
                embedding_dimension=int(get("EMBEDDING_DIMENSION", defaults.embedding_dimension)),
                embedding_failure_rate=float(
                    get("EMBEDDING_FAILURE_RATE", defaults.embedding_failure_rate)
                ),
                max_retries=int(get("MAX_RETRIES", defaults.max_retries)),
                retry_base_delay_seconds=float(
                    get("RETRY_BASE_DELAY_SECONDS", defaults.retry_base_delay_seconds)
                ),
                reconcile_interval_seconds=float(
                    get("RECONCILE_INTERVAL_SECONDS", defaults.reconcile_interval_seconds)
                ),
                reconcile_grace_seconds=float(
                    get("RECONCILE_GRACE_SECONDS", defaults.reconcile_grace_seconds)
                ),
                relay_poll_interval_ms=int(
                    get("RELAY_POLL_INTERVAL_MS", defaults.relay_poll_interval_ms)
                ),
                metrics_port=int(get("METRICS_PORT", defaults.metrics_port)),
            )
        except ValueError as exc:
            if isinstance(exc, ConfigError):
                raise
            raise ConfigError(f"invalid setting: {exc}") from exc
