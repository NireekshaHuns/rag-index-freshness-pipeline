"""Vector similarity search over indexed chunks."""

import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from freshness.embeddings import EmbeddingProvider, TransientEmbeddingError
from freshness.vectors import to_pgvector

router = APIRouter(tags=["search"])

# Deleted documents are excluded even if a stray chunk survives, so search
# never serves content the source no longer has.
SEARCH = """
    SELECT c.document_id, d.title, s.indexed_version, c.position, c.content,
           1 - (c.embedding <=> %(query)s::vector) AS score
    FROM chunks c
    JOIN documents d ON d.id = c.document_id AND NOT d.deleted
    JOIN index_state s ON s.document_id = c.document_id AND NOT s.deleted
    ORDER BY c.embedding <=> %(query)s::vector
    LIMIT %(top_k)s
"""

# Within one document: an exact scan of its chunks. MATERIALIZED keeps the
# planner from routing a highly selective filter through the approximate index.
SEARCH_DOCUMENT = """
    WITH candidates AS MATERIALIZED (
        SELECT document_id, position, content, embedding
        FROM chunks WHERE document_id = %(document_id)s
    )
    SELECT c.document_id, d.title, s.indexed_version, c.position, c.content,
           1 - (c.embedding <=> %(query)s::vector) AS score
    FROM candidates c
    JOIN documents d ON d.id = c.document_id AND NOT d.deleted
    JOIN index_state s ON s.document_id = c.document_id AND NOT s.deleted
    ORDER BY c.embedding <=> %(query)s::vector
    LIMIT %(top_k)s
"""

# A wider candidate list improves recall; iterative scanning keeps fetching
# candidates when filters (deleted documents) discard some, so top_k is filled.
HNSW_SETTINGS = (
    "SET LOCAL hnsw.ef_search = 100",
    "SET LOCAL hnsw.iterative_scan = strict_order",
)


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=50)
    document_id: uuid.UUID | None = Field(
        default=None, description="Only search within this document."
    )


class SearchHit(BaseModel):
    document_id: uuid.UUID
    title: str
    document_version: int
    position: int
    content: str
    score: float


class SearchResponse(BaseModel):
    query: str
    results: list[SearchHit]


@router.post("/search")
def search(body: SearchRequest, request: Request) -> SearchResponse:
    provider: EmbeddingProvider = request.app.state.embedding_provider
    try:
        [vector] = provider.embed([body.query])
    except TransientEmbeddingError as exc:
        raise HTTPException(status_code=503, detail="embedding service unavailable") from exc

    params = {"query": to_pgvector(vector), "top_k": body.top_k, "document_id": body.document_id}
    with request.app.state.pool.connection() as conn, conn.transaction():
        if body.document_id is None:
            for setting in HNSW_SETTINGS:
                conn.execute(setting)
            rows = conn.execute(SEARCH, params).fetchall()
        else:
            rows = conn.execute(SEARCH_DOCUMENT, params).fetchall()
    results = [
        SearchHit(
            document_id=row[0],
            title=row[1],
            document_version=row[2],
            position=row[3],
            content=row[4],
            score=row[5],
        )
        for row in rows
    ]
    return SearchResponse(query=body.query, results=results)
