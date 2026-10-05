"""FastAPI application factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from freshness.api import documents, search
from freshness.config import Settings
from freshness.db import create_pool, migrate
from freshness.embeddings import EmbeddingProvider, create_provider


def create_app(
    settings: Settings | None = None, embedding_provider: EmbeddingProvider | None = None
) -> FastAPI:
    settings = settings or Settings.from_env()
    provider = embedding_provider or create_provider(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        migrate(settings.database_url, settings)
        app.state.pool = create_pool(settings)
        app.state.embedding_provider = provider
        try:
            yield
        finally:
            app.state.pool.close()

    app = FastAPI(title="Freshness API", lifespan=lifespan)
    app.include_router(documents.router)
    app.include_router(search.router)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
