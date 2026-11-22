"""Cold path: Kafka engine.measurements.v1 -> Parquet Bronze on HDFS (lot A4).

    spark-submit streaming/archive.py

- Reads Kafka with Structured Streaming; `startingOffsets=earliest` only matters on the
  very first start: an existing checkpoint always wins.
- Keeps every valid event with its envelope and Kafka provenance (partition, offset,
  Kafka timestamp, ingestion time). No RUL is computed here.
- Appends Parquet to hdfs://namenode:8020/smartpredict/bronze/engine_events/, partitioned by
  source_split, through the file sink (exactly-once with its checkpoint). Replays of the
  same events are appended again on purpose; Silver (Reda) deduplicates.
- Invalid messages never reach Bronze; they are counted per micro-batch with
  `observe()` and logged from the query progress.
- Exclusive checkpoint: hdfs://namenode:8020/smartpredict/checkpoints/archive_v1/.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.events import EVENT_FIELDS, parse_kafka

HDFS = "hdfs://namenode:8020/smartpredict"
BRONZE_PATH = f"{HDFS}/bronze/engine_events/"
CHECKPOINT = f"{HDFS}/checkpoints/archive_v1/"
QUALITY_METRICS = "archive_quality"

BRONZE_COLUMNS = (
    *EVENT_FIELDS,
    "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingested_at",
)


def build_query(spark: SparkSession, args: argparse.Namespace):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
    )
    events = parse_kafka(raw).observe(
        QUALITY_METRICS,
        F.count(F.lit(1)).alias("received"),
        F.sum(F.when(F.col("is_valid"), 0).otherwise(1)).alias("rejected"),
    )
    bronze = events.where("is_valid").withColumn("ingested_at", F.current_timestamp()).select(*BRONZE_COLUMNS)
    return (
        bronze.writeStream.queryName("archive_v1")
        .format("parquet")
        .outputMode("append")
        .partitionBy("source_split")
        .option("path", args.bronze_path)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )


def report(progress) -> None:
    if not progress:
        return
    metrics = (progress.get("observedMetrics") or {}).get(QUALITY_METRICS) or {}
    line = {
        "batch": progress.get("batchId"),
        "input_rows": progress.get("numInputRows"),
        "received": metrics.get("received"),
        "rejected": metrics.get("rejected"),
        "sink": progress.get("sink", {}).get("description"),
    }
    print(f"[archive_v1] {json.dumps(line)}", flush=True)
    if metrics.get("rejected"):
        print(f"[archive_v1] WARNING {metrics['rejected']} invalid event(s) excluded from Bronze", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "engine.measurements.v1"))
    parser.add_argument("--bronze-path", default=BRONZE_PATH)
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument("--allow-local", action="store_true", help="development only: accept a local[*] master")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("smartpredict-archive-v1").getOrCreate()
    master = spark.sparkContext.master
    if master.startswith("local") and not args.allow_local:
        print(f"[archive_v1] ERROR master is {master}; the contract requires spark://spark-master:7077", file=sys.stderr)
        return 2
    print(f"[archive_v1] master={master} topic={args.topic} bronze={args.bronze_path} checkpoint={args.checkpoint}",
          flush=True)

    query = build_query(spark, args)

    def stop(signum, _frame):
        print(f"[archive_v1] signal {signum}: stopping query, checkpoint kept", flush=True)
        query.stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_seen = (-1, False)
    while query.isActive:
        query.awaitTermination(15)
        # recentProgress, not lastProgress: several batches can finish between two polls.
        # An idle trigger reports the NEXT batchId with 0 rows, before the data batch with the
        # same id, so the key also orders "has rows" after "idle".
        for progress in query.recentProgress:
            seen = (progress.get("batchId", -1), (progress.get("numInputRows") or 0) > 0)
            if seen > last_seen:
                last_seen = seen
                report(progress)

    error = query.exception()
    if error is not None:
        print(f"[archive_v1] ERROR query failed: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
