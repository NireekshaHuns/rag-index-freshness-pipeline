"""FastAPI application factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from freshness.api import documents
from freshness.config import Settings
from freshness.db import create_pool, migrate


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        migrate(settings.database_url, settings)
        app.state.pool = create_pool(settings)
        try:
            yield
        finally:
            app.state.pool.close()

    app = FastAPI(title="Freshness Document API", lifespan=lifespan)
    app.include_router(documents.router)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
