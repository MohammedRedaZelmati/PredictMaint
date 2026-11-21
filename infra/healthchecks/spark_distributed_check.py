"""Prove that Spark jobs run on both standalone workers and can read HDFS.

    docker compose run --rm --no-deps spark-client spark-submit infra/healthchecks/spark_distributed_check.py
"""

import socket
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smartpredict-distributed-check").getOrCreate()
sc = spark.sparkContext
print(f"[check] master={sc.master}")
if not sc.master.startswith("spark://"):
    sys.exit("[check] FAILED: not running on the standalone cluster")

hosts = (
    sc.parallelize(range(400), 8)
    .map(lambda _: socket.gethostname())
    .countByValue()
)
print(f"[check] tasks per executor host: {dict(hosts)}")
text = spark.read.text("hdfs://namenode:8020/smartpredict/replication_check.txt").first()[0]
print(f"[check] HDFS read: {text!r}")
spark.stop()
if len(hosts) < 2:
    sys.exit("[check] FAILED: tasks ran on fewer than 2 executor hosts")
print("[check] OK: work distributed over 2 executor hosts and HDFS readable")
