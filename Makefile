COMPOSE ?= docker compose
export COMPOSE

.PHONY: install lint format test up down reset smoke

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

# kafka-init runs in the foreground so topics exist before anything uses them.
up:
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm kafka-init > /dev/null
	./scripts/smoke.sh

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

smoke:
	./scripts/smoke.sh
