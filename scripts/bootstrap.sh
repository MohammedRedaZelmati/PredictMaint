#!/usr/bin/env bash
# Idempotent init of Kafka topic, HDFS layout and PostgreSQL schema.
# Usage (repo root):  docker compose run --rm spark-client bash scripts/bootstrap.sh [--strict]
# Never deletes topics, HDFS data, checkpoints, models or SQL rows; safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 infra/bootstrap.py "$@"
