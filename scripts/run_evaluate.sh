#!/usr/bin/env bash
set -euo pipefail

spark-submit --master spark://spark-master:7077 batch/evaluate.py \
  --stage-reference data/raw/RUL_FD001.txt
