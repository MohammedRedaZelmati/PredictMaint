#!/usr/bin/env bash
# Post-start verification of the V1 cluster. Read-only: changes nothing.
# Run from the repo root:  bash infra/healthchecks/check_cluster.sh
set -u

echo "== Containers =="
docker compose ps

echo
echo "== HDFS: live DataNodes (expect 2) and replication default (expect 2) =="
docker compose exec -T namenode hdfs dfsadmin -report | grep -E "Live datanodes|Name:|Configured Capacity" || true
docker compose exec -T namenode hdfs getconf -confKey dfs.replication || true

echo
echo "== Spark: registered workers (expect 2 ALIVE) =="
if command -v curl >/dev/null 2>&1; then
  curl -s http://localhost:8080/json/ | grep -oE '"(aliveworkers|cores|memory)" *: *[0-9]+' || true
else
  echo "curl missing on host; open http://localhost:8080"
fi

echo
echo "== Kafka: topic list =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || true

echo
echo "== PostgreSQL =="
docker compose exec -T postgres pg_isready || true
