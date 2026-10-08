"""HTTP routes for documents."""

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from freshness.api import store

router = APIRouter(prefix="/documents", tags=["documents"])


class DocumentIn(BaseModel):
    title: str = Field(min_length=1)
    content: str


class DocumentOut(BaseModel):
    id: uuid.UUID
    title: str
    content: str
    version: int
    updated_at: datetime

    @classmethod
    def of(cls, doc: store.Document) -> "DocumentOut":
        return cls(
            id=doc.id,
            title=doc.title,
            content=doc.content,
            version=doc.version,
            updated_at=doc.updated_at,
        )


class ChunkOut(BaseModel):
    position: int
    content: str
    embedded_at_version: int


class IndexStatusOut(BaseModel):
    """Where one document is in the pipeline. All timestamps come from the
    database clock, so the gaps between them are real latencies."""

    id: uuid.UUID
    title: str
    version: int
    deleted: bool
    updated_at: datetime
    published_at: datetime | None
    indexed_version: int | None
    indexed_at: datetime | None
    chunks: list[ChunkOut]

    @classmethod
    def of(cls, status: store.IndexStatus) -> "IndexStatusOut":
        doc = status.document
        return cls(
            id=doc.id,
            title=doc.title,
            version=doc.version,
            deleted=doc.deleted,
            updated_at=doc.updated_at,
            published_at=status.published_at,
            indexed_version=status.indexed_version,
            indexed_at=status.indexed_at,
            chunks=[
                ChunkOut(
                    position=c.position,
                    content=c.content,
                    embedded_at_version=c.embedded_at_version,
                )
                for c in status.chunks
            ],
        )


class DeletedOut(BaseModel):
    id: uuid.UUID
    version: int
    deleted: bool = True


def pool(request: Request) -> ConnectionPool:
    return request.app.state.pool


def not_found(document_id: uuid.UUID) -> HTTPException:
    return HTTPException(status_code=404, detail=f"document {document_id} not found")


@router.post("", status_code=201)
def create(body: DocumentIn, request: Request) -> DocumentOut:
    with pool(request).connection() as conn:
        return DocumentOut.of(store.create_document(conn, body.title, body.content))


@router.get("/{document_id}")
def read(document_id: uuid.UUID, request: Request) -> DocumentOut:
    with pool(request).connection() as conn:
        doc = store.get_document(conn, document_id)
    if doc is None:
        raise not_found(document_id)
    return DocumentOut.of(doc)


@router.get("/{document_id}/index")
def index_status(document_id: uuid.UUID, request: Request) -> IndexStatusOut:
    with pool(request).connection() as conn:
        status = store.get_index_status(conn, document_id)
    if status is None:
        raise not_found(document_id)
    return IndexStatusOut.of(status)


@router.put("/{document_id}")
def update(document_id: uuid.UUID, body: DocumentIn, request: Request) -> DocumentOut:
    with pool(request).connection() as conn:
        doc = store.update_document(conn, document_id, body.title, body.content)
    if doc is None:
        raise not_found(document_id)
    return DocumentOut.of(doc)


@router.delete("/{document_id}")
def delete(document_id: uuid.UUID, request: Request) -> DeletedOut:
    with pool(request).connection() as conn:
        doc = store.delete_document(conn, document_id)
    if doc is None:
        raise not_found(document_id)
    return DeletedOut(id=doc.id, version=doc.version)
