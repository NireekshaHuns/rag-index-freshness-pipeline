#!/usr/bin/env bash
# Confirms the local infrastructure is up and usable.
set -euo pipefail

compose() { ${COMPOSE:-docker compose} "$@"; }

echo "postgres: checking pgvector extension"
compose exec -T postgres psql -U freshness -d freshness -v ON_ERROR_STOP=1 -tAc \
  "SET client_min_messages = warning; CREATE EXTENSION IF NOT EXISTS vector; SELECT '[1,2,3]'::vector <=> '[1,2,4]'::vector;" > /dev/null

echo "kafka: checking topics"
topics=$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list)
for topic in document-changes document-changes.dlq; do
  if ! grep -qx "$topic" <<< "$topics"; then
    echo "missing topic: $topic" >&2
    exit 1
  fi
done

partitions=$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --describe --topic document-changes \
  | grep -c "Partition: ")
echo "kafka: document-changes has $partitions partitions"

echo "api: checking health"
curl -sf http://localhost:8000/health > /dev/null

echo "prometheus: checking readiness"
curl -sf http://localhost:9090/-/ready > /dev/null

echo "grafana: checking provisioned dashboard"
if ! curl -sf "http://localhost:3000/api/dashboards/uid/rag-freshness" > /dev/null; then
  echo "freshness dashboard not provisioned" >&2
  exit 1
fi
echo "grafana: dashboard at http://localhost:3000/d/rag-freshness"

echo "smoke check passed"
