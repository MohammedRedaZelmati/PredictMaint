#!/usr/bin/env bash
# Replay FD001 test split to Kafka. Start spark-inference BEFORE this: on a fresh checkpoint
# the hot path reads from `latest`, so events published earlier would never be scored.
# Usage: docker compose run --rm spark-client bash scripts/run_test_replay.sh [extra producer args]
#   SPEED=5 ...          simulated cycles per second (default 5, readable on the dashboard)
#   --run-id <uuid>      deliberately resume an earlier session
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 -m ingestion.producer --split test --speed "${SPEED:-5}" \
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" "$@"
