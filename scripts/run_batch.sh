#!/usr/bin/env bash
set -euo pipefail

spark-submit --master spark://spark-master:7077 batch/prepare.py
spark-submit --master spark://spark-master:7077 batch/kpis.py
spark-submit --master spark://spark-master:7077 batch/publish_kpis.py
