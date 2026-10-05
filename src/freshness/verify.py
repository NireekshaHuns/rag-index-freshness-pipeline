"""Consistency checks: does the index exactly match the source documents?

This is the ground truth for every guarantee the pipeline makes, so it
recomputes everything from scratch instead of trusting any pipeline state.
"""

import math
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import psycopg

from freshness.chunking import DEFAULT_CONFIG, ChunkingConfig, chunk_document
from freshness.embeddings import EmbeddingProvider
from freshness.hashing import content_hash


@dataclass(frozen=True)
class Violation:
    kind: str
    document_id: uuid.UUID
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.document_id}: {self.detail}"


@dataclass
class VerifyReport:
    live_documents: int = 0
    deleted_documents: int = 0
    chunks: int = 0
    violations: list[Violation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def counts(self) -> dict[str, int]:
        totals: dict[str, int] = defaultdict(int)
        for violation in self.violations:
            totals[violation.kind] += 1
        return dict(sorted(totals.items()))


@dataclass(frozen=True)
class _Chunk:
    content_hash: str
    position: int
    content: str
    embedding: list[float] | None


def verify(
    conn: psycopg.Connection,
    chunking: ChunkingConfig = DEFAULT_CONFIG,
    embeddings: EmbeddingProvider | None = None,
) -> VerifyReport:
    """Check the whole index. Pass a deterministic provider (the fake one) to also
    confirm every stored vector belongs to its chunk's text."""
    # One snapshot so the checks see a single consistent point in time.
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        documents = conn.execute("SELECT id, version, deleted, content FROM documents").fetchall()
        states = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT document_id, indexed_version, deleted FROM index_state"
            ).fetchall()
        }
        chunks: dict[uuid.UUID, list[_Chunk]] = defaultdict(list)
        embedding_column = "embedding::text" if embeddings else "NULL"
        for doc_id, digest, position, content, vector in conn.execute(
            f"SELECT document_id, content_hash, position, content, {embedding_column} FROM chunks"
        ):
            chunks[doc_id].append(_Chunk(digest, position, content, _parse(vector)))

    report = VerifyReport(chunks=sum(len(c) for c in chunks.values()))
    known = set()
    for doc_id, version, deleted, content in documents:
        known.add(doc_id)
        state = states.get(doc_id)
        stored = chunks.get(doc_id, [])
        if deleted:
            report.deleted_documents += 1
            _check_deleted(report, doc_id, version, state, stored)
        else:
            report.live_documents += 1
            _check_live(report, doc_id, version, content, state, stored, chunking, embeddings)

    for doc_id, stored in chunks.items():
        if doc_id not in known:
            report.violations.append(
                Violation("orphan_chunk", doc_id, f"{len(stored)} chunk(s) for unknown document")
            )
    return report


def _check_deleted(
    report: VerifyReport,
    doc_id: uuid.UUID,
    version: int,
    state: tuple[int, bool] | None,
    stored: list[_Chunk],
) -> None:
    if stored:
        report.violations.append(
            Violation("deleted_has_chunks", doc_id, f"{len(stored)} chunk(s) remain")
        )
    if state is None or state != (version, True):
        report.violations.append(
            Violation(
                "delete_not_recorded", doc_id, f"index_state={state}, expected ({version}, True)"
            )
        )


def _check_live(
    report: VerifyReport,
    doc_id: uuid.UUID,
    version: int,
    content: str,
    state: tuple[int, bool] | None,
    stored: list[_Chunk],
    chunking: ChunkingConfig,
    embeddings: EmbeddingProvider | None,
) -> None:
    add = report.violations.append
    if state is None:
        add(Violation("missing_index_state", doc_id, f"document v{version} never indexed"))
    elif state[1]:
        add(Violation("wrongly_deleted", doc_id, "index_state marks a live document deleted"))
    elif state[0] != version:
        add(Violation("version_mismatch", doc_id, f"indexed v{state[0]}, document v{version}"))

    for chunk in stored:
        if content_hash(chunk.content) != chunk.content_hash:
            add(Violation("hash_mismatch", doc_id, f"chunk at {chunk.position} has a wrong hash"))
    positions = [c.position for c in stored]
    if len(positions) != len(set(positions)):
        add(Violation("duplicate_position", doc_id, f"positions {sorted(positions)}"))

    expected = {c.content_hash: c for c in chunk_document(content, chunking)}
    actual = {c.content_hash: c for c in stored}
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    if missing:
        add(Violation("missing_chunks", doc_id, f"{len(missing)} expected chunk(s) not indexed"))
    if extra:
        add(Violation("stale_chunks", doc_id, f"{len(extra)} chunk(s) not in current content"))
    moved = [
        h for h in expected.keys() & actual.keys() if expected[h].position != actual[h].position
    ]
    if moved:
        add(Violation("position_mismatch", doc_id, f"{len(moved)} chunk(s) at the wrong position"))

    if embeddings:
        _check_embeddings(report, doc_id, stored, embeddings)


def _check_embeddings(
    report: VerifyReport, doc_id: uuid.UUID, stored: list[_Chunk], provider: EmbeddingProvider
) -> None:
    if not stored:
        return
    vectors = provider.embed([c.content for c in stored])
    for chunk, expected in zip(stored, vectors, strict=True):
        assert chunk.embedding is not None
        # pgvector stores float4, so compare with a tolerance.
        if len(chunk.embedding) != len(expected) or not all(
            math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-6)
            for a, b in zip(chunk.embedding, expected, strict=True)
        ):
            report.violations.append(
                Violation("embedding_mismatch", doc_id, f"chunk at {chunk.position}")
            )


def _parse(vector: str | None) -> list[float] | None:
    return None if vector is None else [float(v) for v in vector.strip("[]").split(",")]
