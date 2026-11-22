#!/usr/bin/env bash
# Replay FD001 train split to Kafka. Start spark-archive BEFORE this (cold path, earliest offsets).
# Usage: docker compose run --rm spark-client bash scripts/run_train_replay.sh [extra producer args]
#   SPEED=50 ...         simulated cycles per second (default 50: 362 cycle steps => ~7.5 s minimum)
#   --engine-ids 1,2,3   small smoke test first, before the full 100-engine replay.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 -m ingestion.producer --split train --speed "${SPEED:-50}" \
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" "$@"
