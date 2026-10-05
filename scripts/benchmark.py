"""Benchmark: seed documents, edit a share of their paragraphs, and measure how
fresh the index stays and how much embedding work incremental indexing saves.

Runs against a live stack (`make up`); results go to docs/benchmarks.md.
"""

import argparse
import json
import math
import platform
import random
import time
import urllib.request
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

DEFAULT_REPORT = Path(__file__).resolve().parents[1] / "docs" / "benchmarks.md"
TOPICS = [
    "billing", "security", "onboarding", "refunds", "shipping", "privacy",
    "support", "pricing", "travel", "payroll", "hiring", "compliance",
]  # fmt: skip


def paragraph(rng: random.Random, doc: int, index: int, revision: int = 0) -> str:
    """A unique paragraph comfortably above the chunker's merge threshold."""
    topic = rng.choice(TOPICS)
    sentences = [
        f"Section {index} of document {doc} covers {topic} policy revision {revision}.",
        *(
            f"Rule {rng.randrange(10_000)} sets the {topic} limit to {rng.randrange(1000)} units."
            for _ in range(rng.randint(2, 4))
        ),
    ]
    return " ".join(sentences)


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile; values need not be sorted."""
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def choose_edits(
    paragraph_counts: list[int], fraction: float, rng: random.Random
) -> dict[int, list[int]]:
    """Pick `fraction` of all paragraphs, grouped by document index."""
    all_paragraphs = [(d, p) for d, count in enumerate(paragraph_counts) for p in range(count)]
    picked = rng.sample(all_paragraphs, round(len(all_paragraphs) * fraction))
    edits: dict[int, list[int]] = defaultdict(list)
    for doc, para in picked:
        edits[doc].append(para)
    return dict(edits)


class Api:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def call(self, method: str, path: str, body: dict[str, Any]) -> Any:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body).encode(),
            method=method,
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)


@dataclass
class Phase:
    name: str
    documents: int
    seconds: float
    lags: list[float]
    chunks_embedded: int
    chunks_total: int

    @property
    def throughput(self) -> float:
        return self.documents / self.seconds if self.seconds else math.nan


def wait_indexed(conn: psycopg.Connection, ids: list[uuid.UUID], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        row = conn.execute(
            """
            SELECT count(*) FROM documents d JOIN index_state s ON s.document_id = d.id
            WHERE d.id = ANY(%s) AND s.indexed_version = d.version
            """,
            (ids,),
        ).fetchone()
        done = row[0] if row else 0
        if done == len(ids):
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out: {done}/{len(ids)} documents indexed")
        time.sleep(0.25)


def measure(conn: psycopg.Connection, name: str, ids: list[uuid.UUID]) -> Phase:
    """Exact numbers from the database: per-document lag uses one clock for both
    ends, and chunks.document_version marks chunks embedded at that version."""
    lags = [
        row[0]
        for row in conn.execute(
            """
            SELECT extract(epoch FROM s.indexed_at - d.updated_at)::float8
            FROM documents d JOIN index_state s ON s.document_id = d.id
            WHERE d.id = ANY(%s)
            """,
            (ids,),
        )
    ]
    span = conn.execute(
        """
        SELECT extract(epoch FROM max(s.indexed_at) - min(d.updated_at))::float8
        FROM documents d JOIN index_state s ON s.document_id = d.id WHERE d.id = ANY(%s)
        """,
        (ids,),
    ).fetchone()
    chunks = conn.execute(
        """
        SELECT count(*) FILTER (WHERE c.document_version = d.version), count(*)
        FROM chunks c JOIN documents d ON d.id = c.document_id WHERE d.id = ANY(%s)
        """,
        (ids,),
    ).fetchone()
    assert span is not None and chunks is not None
    return Phase(name, len(ids), span[0], lags, chunks[0], chunks[1])


def run(args: argparse.Namespace) -> tuple[Phase, Phase, dict[str, Any]]:
    rng = random.Random(args.seed)
    api = Api(args.api_url)
    run_id = uuid.uuid4().hex[:6]
    counts = [rng.randint(args.min_paragraphs, args.max_paragraphs) for _ in range(args.documents)]
    contents = [[paragraph(rng, d, p) for p in range(n)] for d, n in enumerate(counts)]

    print(f"seeding {args.documents} documents ({sum(counts)} paragraphs)...")
    with ThreadPoolExecutor(args.concurrency) as pool:
        created = list(
            pool.map(
                lambda d: api.call(
                    "POST",
                    "/documents",
                    {"title": f"bench-{run_id}-{d}", "content": "\n\n".join(contents[d])},
                )["id"],
                range(args.documents),
            )
        )
    ids = [uuid.UUID(i) for i in created]

    with psycopg.connect(args.database_url, autocommit=True) as conn:
        wait_indexed(conn, ids, args.timeout)
        seed = measure(conn, "seed", ids)
        print(f"  indexed in {seed.seconds:.1f}s")

        edits = choose_edits(counts, args.edit_fraction, rng)
        edited_ids = [ids[d] for d in edits]
        print(
            f"editing {sum(len(p) for p in edits.values())} paragraphs "
            f"({args.edit_fraction:.0%}) across {len(edits)} documents at {args.rate}/s..."
        )

        def edit(doc: int) -> None:
            for para in edits[doc]:
                contents[doc][para] = paragraph(rng, doc, para, revision=1)
            api.call(
                "PUT",
                f"/documents/{ids[doc]}",
                {"title": f"bench-{run_id}-{doc}", "content": "\n\n".join(contents[doc])},
            )

        interval = 1 / args.rate
        started = time.monotonic()
        with ThreadPoolExecutor(args.concurrency) as pool:
            futures = []
            for n, doc in enumerate(edits):
                # Paced submission so the lag reflects steady-state freshness,
                # not just how long a giant burst takes to drain.
                time.sleep(max(0.0, started + n * interval - time.monotonic()))
                futures.append(pool.submit(edit, doc))
            for future in futures:
                future.result()
        wait_indexed(conn, edited_ids, args.timeout)
        edit_phase = measure(conn, "edit", edited_ids)

    meta = {
        "date": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "documents": args.documents,
        "paragraphs": sum(counts),
        "edited_paragraphs": sum(len(p) for p in edits.values()),
        "edit_fraction": args.edit_fraction,
        "rate": args.rate,
        "machine": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
    }
    return seed, edit_phase, meta


def render(seed: Phase, edit: Phase, meta: dict[str, Any], command: str) -> str:
    saved = 1 - edit.chunks_embedded / edit.chunks_total if edit.chunks_total else math.nan

    def lag_row(phase: Phase) -> str:
        p50, p95, p99 = (percentile(phase.lags, p) for p in (50, 95, 99))
        return (
            f"| {phase.name} | {phase.documents} | {p50 * 1000:.0f} ms | {p95 * 1000:.0f} ms "
            f"| {p99 * 1000:.0f} ms | {max(phase.lags) * 1000:.0f} ms "
            f"| {phase.throughput:.1f} docs/s |"
        )

    return f"""# Benchmarks

Generated by `{command}` on {meta["date"]}.

## Setup

- Full Docker Compose stack: API, 1 relay, 2 indexer replicas, reconciler,
  Postgres 16 + pgvector, single-broker Kafka (6 partitions).
- Fake embedding provider (deterministic, local). Freshness numbers therefore
  exclude embedding API latency; the re-embedding savings do not depend on the
  provider.
- {meta["documents"]} documents, {meta["paragraphs"]} paragraphs. The edit phase
  rewrote {meta["edited_paragraphs"]} randomly chosen paragraphs
  ({meta["edit_fraction"]:.0%} of all paragraphs) across {edit.documents} documents,
  one update per document, paced at {meta["rate"]} updates/s.
- Host: {meta["machine"]}.

Lag is measured per document in the database as
`index_state.indexed_at - documents.updated_at`: from the change commit to the
index commit, both on the database clock. Throughput is documents indexed
divided by the time from the first change to the last index commit.

## Freshness and throughput

| Phase | Documents | p50 lag | p95 lag | p99 lag | Max lag | Throughput |
|---|---|---|---|---|---|---|
{lag_row(seed)}
{lag_row(edit)}

The seed phase writes every document at once, so its lag mostly measures how
long the burst takes to drain. The edit phase is paced and reflects
steady-state freshness.

## Re-embedding

| | Chunks |
|---|---|
| Chunks in the edited documents | {edit.chunks_total} |
| Re-embedded by this pipeline | {edit.chunks_embedded} ({1 - saved:.1%}) |
| Re-embedded by a full per-document re-index | {edit.chunks_total} (100%) |
| Embedding calls avoided | {edit.chunks_total - edit.chunks_embedded} ({saved:.1%}) |

Only paragraphs whose text changed were embedded again; every unchanged chunk
kept its stored embedding.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--documents", type=int, default=1000)
    parser.add_argument("--min-paragraphs", type=int, default=4)
    parser.add_argument("--max-paragraphs", type=int, default=12)
    parser.add_argument("--edit-fraction", type=float, default=0.10)
    parser.add_argument("--rate", type=float, default=50.0, help="edit updates per second")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument(
        "--database-url", default="postgresql://freshness:freshness@localhost:5433/freshness"
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    seed, edit, meta = run(args)
    report = render(seed, edit, meta, "make bench")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    print()
    print(report)
    print(f"written to {args.output}")


if __name__ == "__main__":
    main()
