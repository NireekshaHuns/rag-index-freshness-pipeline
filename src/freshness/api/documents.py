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
