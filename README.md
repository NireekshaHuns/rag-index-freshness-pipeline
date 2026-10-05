# RAG Index Freshness Pipeline

[![CI](https://github.com/NireekshaHuns/rag-index-freshness-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/NireekshaHuns/rag-index-freshness-pipeline/actions/workflows/ci.yml)

An event-driven pipeline that keeps a RAG vector index in sync with its source
documents within a second of a change. Only the paragraphs that actually changed
are re-embedded. No update is lost or applied twice, even when processes crash,
events are duplicated or reordered, or the embedding API fails.

**Measured** (1,000 documents, 10% of paragraphs edited; [details](docs/benchmarks.md)):
p50 freshness lag **168 ms**, p95 **682 ms**, and **83% fewer embedding calls**
than re-indexing each edited document.

## The problem

RAG applications answer questions from document chunks stored in a vector
index. When a source document is edited or deleted, the index goes stale and the
application keeps answering confidently from outdated text. Common fixes are
scheduled batch re-scans, which leave the index stale for hours, and they often
re-embed whole documents when one paragraph changed.

This project treats index freshness as a data-consistency problem: every
committed change must reach the index quickly, exactly once in effect, and
cheaply.

## Architecture

```mermaid
flowchart LR
    client([Client]) -->|POST/PUT/DELETE| api[Document API]
    api -->|one transaction| pg[(PostgreSQL<br/>documents + outbox)]
    relay[Outbox relay] -->|"poll<br/>FOR UPDATE SKIP LOCKED"| pg
    relay -->|"key = document_id"| kafka{{Kafka<br/>document-changes}}
    kafka --> idx1[Indexer 1]
    kafka --> idx2[Indexer 2]
    idx1 & idx2 -->|"read current doc,<br/>diff chunks"| pg
    idx1 & idx2 -->|embed changed chunks only| emb[Embedding provider]
    idx1 & idx2 -->|"one transaction:<br/>chunks + index_state"| vec[(pgvector<br/>chunks)]
    idx1 & idx2 -.->|after retries| dlq{{document-changes.dlq}}
    rec[Reconciler] -.->|"re-enqueue stale docs<br/>remove orphans"| pg
    client -->|POST /search| api
    api -->|cosine similarity| vec
    prom[Prometheus] -.->|scrape /metrics| api & relay & idx1 & idx2 & rec
    graf[Grafana] --> prom
```

Postgres holds the documents, the outbox, and the chunk vectors (via pgvector),
so a document change and its event, or a chunk change and its index state, can
always be committed together.

| Component | Role |
|---|---|
| **Document API** (FastAPI) | Writes the document and an outbox row in one transaction. Serves `POST /search` (optionally scoped to one `document_id`). |
| **Outbox relay** | Publishes unpublished outbox rows to Kafka, keyed by `document_id`. It marks a row published only after Kafka acknowledges it. |
| **Kafka** (KRaft, 6 partitions) | Ordered, replayable log. All events for one document land on one partition. |
| **Indexer** (consumer group, 2 replicas) | Reads the *current* document, chunks it, diffs by content hash, embeds only new chunks, and applies the result in one transaction. It commits the Kafka offset after that. |
| **Dead-letter topic** | Receives events that still fail after bounded retries, with the error details. |
| **Reconciler** | Periodic safety net: re-enqueues documents whose index is behind, and removes orphaned chunks. |
| **Prometheus + Grafana** | Freshness lag, backlog, throughput, embedding reuse, retries, DLQ and drift, provisioned as code. |

## Quick start

**Prerequisites:** Docker with Compose v2 (Docker Desktop or OrbStack),
[`uv`](https://docs.astral.sh/uv/), and `make`. `uv` installs Python 3.12 itself.

```bash
git clone https://github.com/NireekshaHuns/rag-index-freshness-pipeline.git
cd rag-index-freshness-pipeline
make install   # Python dependencies (for the scripts and tests)
make up        # Postgres, Kafka, API, relay, 2 indexers, reconciler, Prometheus, Grafana
make demo      # create -> search -> edit one paragraph -> time until search reflects it
```

`make demo` prints something like:

```
==> Searching the document: 'what is the hotel nightly limit'
  indexed and searchable after 0.70s
  BEFORE (v1, score 0.170):
    "Hotel stays are reimbursed up to a nightly limit of 200 dollars in most cities. ..."
==> Editing one paragraph: 200 -> 250 dollars per night
==> Polling search until the new content appears
  AFTER  (v2, score 0.170):
    "Hotel stays are reimbursed up to a nightly limit of 250 dollars in most cities. ..."
------------------------------------------------------------
  Edit visible in search after 0.14s
  Old text still returned: no
  chunks re-embedded for the edit: 1 of 4 (3 reused)
------------------------------------------------------------
```

Then open:

| URL | What |
|---|---|
| http://localhost:3000/d/rag-freshness | Grafana dashboard (anonymous read-only; admin login `admin` / `admin`) |
| http://localhost:8000/docs | API docs (OpenAPI) |
| http://localhost:9090 | Prometheus |

Postgres is published on host port **5433**, so it doesn't clash with a local
Postgres on 5432. Kafka is on 9092.

### Everyday commands

| Command | What it does |
|---|---|
| `make up` / `make down` / `make reset` | Start everything / stop / stop and delete all data |
| `make demo` | End-to-end freshness demo |
| `make verify` | Check that the index exactly matches the source documents (exits 1 on any violation) |
| `make bench` | Run the benchmark and rewrite `docs/benchmarks.md` |
| `make chaos` | Fault-injection scenarios, each followed by the verifier |
| `make lint` / `make test` | Ruff checks / unit + integration tests (needs Docker) |
| `make logs` / `make ps` | Follow service logs / list containers |
| `make infra` + `make api` / `relay` / `indexer` / `reconciler` | Infrastructure in Docker, services on the host for development |

### Using OpenAI embeddings

The default `fake` provider makes deterministic vectors locally, so everything
runs offline at no cost. To use `text-embedding-3-small`:

```bash
cp .env.example .env
# set EMBEDDING_PROVIDER=openai and OPENAI_API_KEY=sk-...
make reset && make up   # reset only if the vector dimension changed
```

## Guarantees and how each is enforced

| # | Guarantee | Enforced by | Proven by |
|---|---|---|---|
| 1 | **No lost updates.** Every committed change eventually reaches the index. | Transactional outbox: the document write and its event commit together. The relay marks rows published only after Kafka acknowledges. The indexer commits offsets only after its database transaction. Dead-lettered events are re-enqueued by the reconciler. | `test_document_api` (an outbox failure rolls back the write), `test_relay` (crash before marking), chaos: indexer killed, relay killed, 30% embedding failures |
| 2 | **No duplicate effects.** Processing an event twice equals processing it once. | Version check: skip when `indexed_version >= documents.version`. The check is repeated under a per-document advisory lock inside the apply transaction. | `test_indexer` (same event twice), chaos: every event replayed twice; every chunk and `index_state` row unchanged |
| 3 | **Correct final state under reordering.** | Events carry no content. The indexer always reads the current document, so an old event can only ever index the latest version or be skipped. | `test_indexer` (old after new, late upsert after delete), chaos: shuffled events on random partitions, plus the full history replayed backwards |
| 4 | **Minimal re-embedding.** | Paragraph-aware chunking plus chunk identity by content hash. The diff embeds only hashes that aren't already stored. | `test_diffing`, `test_indexer` (editing one paragraph embeds one chunk), benchmark: 806 of 4,817 chunks re-embedded for 806 edited paragraphs |
| 5 | **No orphans.** | Removed chunks are deleted in the same transaction that inserts their replacements. Deletes remove all chunks. The reconciler deletes chunks of missing or deleted documents. Search also filters deleted documents. | `test_reconciler`, `test_verify`, the verifier after every chaos scenario |
| 6 | **Observable freshness.** | `index_freshness_lag_seconds`: from the change commit to the index commit, both timestamps taken on the database clock. Plus backlog, consumer lag, stale-document and orphan gauges. | `test_metrics*`, the Grafana dashboard |

`scripts/verify.py` is the ground truth for guarantees 1–5. It recomputes the
expected chunking of every document from scratch and compares hashes, positions,
versions, deletes, and (with the fake provider) every stored vector.

## Design decisions and trade-offs

**1. Paragraph-aware chunking.** Content is split on blank lines. Paragraphs
under 80 characters (headings, short lines) are merged forward, and paragraphs
over 1,000 characters are split at sentence boundaries. With fixed-size windows,
inserting one sentence shifts every later boundary and forces nearly the whole
document to be re-embedded; a unit test demonstrates this. A full-size paragraph
always starts a new chunk, so a change can only affect nearby chunks.
*Trade-off:* chunk sizes vary with the writing style, and very uneven documents
produce uneven chunks. Token-aware limits would be more precise.

**2. Chunk identity is the content hash, not the position.** Text is normalized
(Unicode NFC, collapsed whitespace) and hashed with SHA-256. A paragraph that
moved keeps its embedding; only its `position` is updated. *Trade-off:*
identical paragraphs within one document are stored once, at their first
position. That's harmless for retrieval, but the chunk count is lower than the
paragraph count.

**3. Read current state; don't trust event payloads.** An event only says
"document X changed at version N". This makes duplicates, delays, and reordering
harmless by construction, and keeps events small. *Trade-off:* every event costs
a database read, and intermediate versions can be skipped. The index reflects
the latest state, not every revision.

**4. Version-based idempotency.** If `index_state.indexed_version >=
documents.version`, the event is a no-op. Deletes are soft and bump the version,
so a late upsert can never resurrect a deleted document. *Trade-off:* deleted
documents remain as tombstone rows.

**5. At-least-once delivery plus idempotent processing gives effectively-once
results.** The relay and the indexer can each redeliver after a crash, and the
version check absorbs the repeats. No distributed transactions or Kafka
transactions are needed.

**6. Partition key = `document_id`.** One document's events stay in order on one
partition, so normally only one worker touches a document at a time.
Correctness doesn't depend on this: the chaos tests deliberately scatter one
document's events across partitions, and the per-document advisory lock keeps
the result correct.

**7. Embed outside the transaction, apply inside it.** Embedding is slow and can
fail, so it happens before any lock is taken. The apply transaction then takes
the per-document lock, re-checks the version, and **re-runs the chunk diff**
against what's stored. If the document changed meanwhile, or another writer
touched its chunks, it retries from the start (bounded). *Trade-off:* a document
under very heavy concurrent editing can need several attempts, so some embedding
work is wasted.

**8. Dead-letter after bounded retries.** Transient failures are retried in
place with exponential backoff and full jitter (5 retries by default). Permanent
errors, such as a bad API key, skip the retries. After the last attempt the
original event goes to `document-changes.dlq` with the error, attempt count,
source offset and traceback in headers, and the offset is committed so the
partition keeps moving. *Trade-off:* retrying in place blocks that partition for
up to about 15 s (with the defaults), the price of keeping per-document order
without a retry topic.

**9. The reconciler is a safety net, not the main path.** Every interval it
counts stale documents, re-enqueues those older than a grace period through the
outbox (so repairs take the same ordered, retried, observable path), and deletes
chunks of missing or deleted documents. In the 30% failure chaos scenario, it's
what brings dead-lettered documents back.

## Benchmark results

From `make bench` on the full Compose stack (2 indexers, fake embeddings, Apple
silicon Mac). Full report: [docs/benchmarks.md](docs/benchmarks.md).

| Phase | Documents | p50 lag | p95 lag | p99 lag | Throughput |
|---|---|---|---|---|---|
| Seed (1,000 docs at once) | 1,000 | 2.9 s | 7.3 s | 7.5 s | 119 docs/s |
| Edit (paced at 50 updates/s) | 560 | 168 ms | 682 ms | 943 ms | 49.8 docs/s |

| Re-embedding for the edit phase | Chunks |
|---|---|
| Chunks in edited documents | 4,817 |
| Re-embedded by this pipeline | 806 (16.7%) |
| Embedding calls avoided vs. re-indexing each edited document | 4,011 (83.3%) |

The seed phase is a burst, so its lag is mostly queueing; the edit phase shows
steady-state freshness. With a real embedding API, each re-embed adds a network
round-trip, so avoiding 83% of the calls matters even more.

## Testing

| Suite | Command | What it covers |
|---|---|---|
| Unit | `make test-unit` | Chunking, hashing, diffing, providers (OpenAI via a mocked transport), retry policy, worker control flow, metrics |
| Integration | `make test-integration` | Real Postgres + pgvector and Kafka via testcontainers: outbox atomicity, relay ordering and crash recovery, indexer idempotency, DLQ, reconciler repairs, search, metrics, verifier |
| Chaos | `make chaos` | Real service processes: indexers SIGKILLed while busy, duplicate events, shuffled and backwards-replayed events, 30% embedding failures, relay SIGKILLed mid-run; the verifier must report zero violations each time and no service may crash unexpectedly |

CI runs lint, unit, and integration tests on every push.

The chaos suite earned its place. It found that a routine consumer-group
rebalance could reject an offset commit (`ILLEGAL_GENERATION`) and crash an
indexer. The index still ended up correct, because the other worker took over,
so only a process-liveness check exposed it. The worker now logs the rejected
commit and continues, and the redelivered event is skipped as already indexed.

## Configuration

All settings are environment variables (see `.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://freshness:freshness@localhost:5433/freshness` | Postgres connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC` | `document-changes` / `document-changes.dlq` | Topics |
| `KAFKA_CONSUMER_GROUP` | `indexer` | Indexer consumer group |
| `KAFKA_SESSION_TIMEOUT_MS` | `10000` | How fast a dead indexer's partitions are reassigned |
| `EMBEDDING_PROVIDER` | `fake` | `fake` or `openai` |
| `OPENAI_API_KEY` | | Required for `openai` |
| `EMBEDDING_DIMENSION` | `384` | Vector size (fixed when the schema is first created) |
| `EMBEDDING_FAILURE_RATE` | `0` | Fraction of embedding calls to fail, for chaos testing |
| `MAX_RETRIES` / `RETRY_BASE_DELAY_SECONDS` | `5` / `0.5` | Retry policy before dead-lettering |
| `RECONCILE_INTERVAL_SECONDS` / `RECONCILE_GRACE_SECONDS` | `300` / `60` | Reconciler cadence, and how old drift must be before repair (Compose uses a 60 s interval) |
| `RELAY_POLL_INTERVAL_MS` | `200` | Relay poll interval when the outbox is empty |
| `METRICS_PORT` | `9100` | `/metrics` port for the relay, indexer, and reconciler |

## Repository layout

```
src/freshness/
  api/            Document API, search, app factory
  relay/          Outbox relay
  indexer/        Processor, consumer loop, retry policy, DLQ publisher
  reconciler/     Drift detection and repair
  embeddings/     Provider interface, fake, OpenAI, failure injection
  chunking.py     Paragraph-aware chunker
  hashing.py      Normalization + SHA-256
  diffing.py      Chunk set diff
  verify.py       Consistency checks (used by scripts/verify.py and the chaos tests)
  metrics.py      All Prometheus series
migrations/       Numbered SQL, applied at service startup under an advisory lock
scripts/          demo.py, verify.py, benchmark.py, smoke.sh
deploy/           Prometheus config, Grafana provisioning and dashboard
tests/            unit/, integration/, chaos/
```

## Known limitations and future work

- **Embedding dimension is fixed at schema creation.** Changing
  `EMBEDDING_DIMENSION` requires `make reset`. A migration path that re-embeds
  into a new column would remove that.
- **Multiple relays can reorder one document's events.** `SKIP LOCKED` lets two
  relays publish neighbouring rows for the same document. This is harmless
  because of decision 3, but strict order would need per-document claiming.
- **Search can mix a new title with old content** in the short window before the
  indexer catches up: the title comes from `documents`, the content from the
  index.
- **The fake embedding provider is a weak retriever on large corpora.** It
  hashes words into 384 dimensions, so with thousands of distinct tokens,
  unrelated text collides with a short query. It exists to make the pipeline
  runnable offline and verifiable, not to rank well. Use
  `EMBEDDING_PROVIDER=openai` for meaningful relevance. The demo scopes its
  search to its own document, so other data can't affect its timing.
- **Approximate search trades recall for speed.** Unscoped search uses the HNSW
  index with `ef_search = 100` and pgvector's iterative scan, so filtered-out
  (deleted) documents don't reduce the result count. Scoped search is an exact
  scan of one document's chunks.
- **The reconciler doesn't re-chunk documents,** so it won't notice a stale chunk
  inside an up-to-date document; only a bug could cause one. `make verify` does
  that full check.
- **Embeddings are one batch per document.** Batching across documents would cut
  API round-trips under a backlog.
- **Single-broker Kafka and an unauthenticated API** are for local use only.
  Production would need replication factor 3, authentication on the API, and
  real Grafana credentials.
- **No DLQ replay tool yet.** Events in the DLQ keep their original key and
  value, so replaying them is a straight copy back to the main topic. The
  reconciler already repairs the affected documents.
- **Future:** change-data capture (for example Debezium) as an alternative to
  the polling relay, token-aware chunk limits, and per-tenant indexes.

## License

MIT. See [LICENSE](LICENSE).
