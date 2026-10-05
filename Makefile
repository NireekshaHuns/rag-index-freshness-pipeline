COMPOSE ?= docker compose
export COMPOSE

.PHONY: install lint format test test-unit test-integration up down reset smoke migrate api relay indexer

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

# kafka-init runs in the foreground so topics exist before anything uses them.
up:
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm kafka-init > /dev/null
	./scripts/smoke.sh

api:
	uv run uvicorn --factory freshness.api.app:create_app --port 8000 --reload

relay:
	uv run python -m freshness.relay

indexer:
	uv run python -m freshness.indexer

migrate:
	uv run python -m freshness migrate

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

smoke:
	./scripts/smoke.sh
