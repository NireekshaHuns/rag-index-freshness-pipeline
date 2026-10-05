CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    version INT NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    document_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('upserted', 'deleted')),
    document_version INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ NULL
);

-- The relay only ever scans unpublished rows, in id order.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_document_idx ON outbox (document_id, document_version);

CREATE TABLE chunks (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL,
    content_hash TEXT NOT NULL,
    position INT NOT NULL,
    content TEXT NOT NULL,
    embedding VECTOR({{EMBEDDING_DIMENSION}}) NOT NULL,
    document_version INT NOT NULL,
    UNIQUE (document_id, content_hash)
);

CREATE INDEX chunks_embedding_hnsw_idx ON chunks USING hnsw (embedding vector_cosine_ops);

CREATE TABLE index_state (
    document_id UUID PRIMARY KEY,
    indexed_version INT NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT false
);
