COMPOSE ?= docker compose
export COMPOSE

.PHONY: install lint format test test-unit test-integration up infra ps logs down reset smoke migrate api relay indexer reconciler

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest

test-unit:
	uv run pytest -m "not integration"

test-integration:
	uv run pytest -m integration

APP_SERVICES = api relay indexer reconciler

# kafka-init runs in the foreground so topics exist before any service starts.
up:
	$(COMPOSE) up -d --wait postgres kafka prometheus grafana
	$(COMPOSE) run --rm kafka-init > /dev/null
	$(COMPOSE) build api
	$(COMPOSE) up -d --wait $(APP_SERVICES)
	./scripts/smoke.sh

infra:
	$(COMPOSE) up -d --wait postgres kafka prometheus grafana
	$(COMPOSE) run --rm kafka-init > /dev/null

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100 $(APP_SERVICES)

api:
	uv run uvicorn --factory freshness.api.app:create_app --port 8000 --reload

relay:
	METRICS_PORT=9101 uv run python -m freshness.relay

indexer:
	METRICS_PORT=9102 uv run python -m freshness.indexer

reconciler:
	METRICS_PORT=9103 uv run python -m freshness.reconciler

migrate:
	uv run python -m freshness migrate

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

smoke:
	./scripts/smoke.sh
