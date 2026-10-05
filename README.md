# RAG Index Freshness Pipeline

An event-driven pipeline that keeps a RAG vector index in sync with its source documents within seconds of a change. Document writes go through a transactional outbox into Kafka, and indexer workers re-embed only the chunks that actually changed, using version checks so that crashes, duplicate deliveries, out-of-order events, and embedding API failures never cause lost updates, duplicate effects, or orphaned chunks. Index freshness is measured and shown on a Grafana dashboard.
