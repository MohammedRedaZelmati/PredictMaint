"""Hot path: Kafka -> Spark ML PipelineModel -> PostgreSQL predictions (lot A5).

    spark-submit streaming/inference.py --model-version v001

Start-up: validate contracts/feature_columns.json and the HDFS manifest, then
PipelineModel.load() from hdfs://namenode:8020/smartpredict/models/rul_pipeline/<version>/.
Any mismatch or missing artefact stops the job with an explicit error (no fallback).

Stream: Kafka `latest` on a fresh checkpoint (start this job BEFORE the test replay); an
existing checkpoint at .../checkpoints/inference_v1/ always wins. Each micro-batch is
validated, scored with model.transform() on the executors, clamped to [0, 125], mapped to
a health status and UPSERTed into `predictions` from foreachPartition. No toPandas(), no
fit(), no heuristic. A database error fails the micro-batch so Spark retries it.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.events import NUMERIC_FIELDS, parse_kafka
from streaming.model_contract import (
    HDFS, ModelContractError, check_manifest, load_feature_columns, manifest_uri, model_uri,
)
from streaming.postgres_writer import (
    CRITICAL_MAX, PREDICTION_COLUMNS, RUL_CAP, WARNING_MAX, connection_params_from_env, write_rows,
)

CHECKPOINT = f"{HDFS}/checkpoints/inference_v1/"
QUALITY_METRICS = "inference_quality"


def read_manifest(spark: SparkSession, uri: str) -> dict:
    try:
        text = spark.sparkContext.wholeTextFiles(uri).values().first()
    except Exception as error:  # noqa: BLE001 - surface any HDFS failure as a contract error
        raise ModelContractError(f"cannot read manifest {uri}: {error}") from error
    return json.loads(text)


def load_model(spark: SparkSession, version: str) -> tuple[PipelineModel, list[str]]:
    features = load_feature_columns()
    check_manifest(read_manifest(spark, manifest_uri(version)), version, features)
    try:
        model = PipelineModel.load(model_uri(version))
    except Exception as error:  # noqa: BLE001
        raise ModelContractError(f"cannot load PipelineModel {model_uri(version)}: {error}") from error
    return model, features


class ConflictingEventError(ValueError):
    """Same (run_id, event_id) with different measurements: V1 duplicate policy says fail."""


def check_no_conflicting_duplicates(batch: DataFrame) -> None:
    """Raise if one (run_id, event_id) carries two different measurement sets in this batch."""
    fingerprint = F.sha2(F.to_json(F.struct(*[F.col(name).cast("double") for name in NUMERIC_FIELDS])), 256)
    conflicts = (
        batch.groupBy("run_id", "event_id")
        .agg(F.countDistinct(fingerprint).alias("versions"))
        .where(F.col("versions") > 1)
        .select("event_id")
        .limit(5)
        .collect()
    )
    if conflicts:
        raise ConflictingEventError(
            f"conflicting measurements for {[row.event_id for row in conflicts]}: micro-batch refused"
        )


def score(batch: DataFrame, model: PipelineModel, features: list[str], version: str) -> DataFrame:
    """Valid events -> rows of the `predictions` table (distributed, no driver collect)."""
    # V1 duplicate policy: identical replays are deduplicated, conflicting ones fail the batch
    # (dropDuplicates alone would keep an arbitrary one of the two measurement sets).
    check_no_conflicting_duplicates(batch)
    # Same event replayed twice inside one micro-batch would make a single UPSERT touch a row twice.
    batch = batch.dropDuplicates(["run_id", "event_id"])
    inputs = batch.withColumns({name: F.col(name).cast("double") for name in features})  # 25 doubles
    scored = model.transform(inputs)
    prediction = F.col("prediction").cast("double")
    # A missing, NaN or infinite prediction stays NULL (never clamped to 0 or 125): the writer
    # then refuses the row and the micro-batch fails, instead of inventing a health status.
    unusable = prediction.isNull() | F.isnan(prediction) | (F.abs(prediction) == F.lit(float("inf")))
    rul = F.when(unusable, F.lit(None).cast("double")).otherwise(
        F.greatest(F.lit(0.0), F.least(F.lit(RUL_CAP), prediction))
    )
    return (
        scored.withColumn("predicted_rul_cycles", rul)
        .withColumn(
            "health_status",
            F.when(F.col("predicted_rul_cycles") <= CRITICAL_MAX, "CRITICAL")
            .when(F.col("predicted_rul_cycles") <= WARNING_MAX, "WARNING")
            .when(F.col("predicted_rul_cycles").isNotNull(), "HEALTHY"),
        )
        .withColumn("model_version", F.lit(version))
        .withColumn("predicted_at", F.current_timestamp())
        .withColumn("engine_id", F.col("engine_id").cast("int"))
        .withColumn("cycle", F.col("cycle").cast("int"))
        .select(*PREDICTION_COLUMNS)
    )


def make_batch_writer(model: PipelineModel, features: list[str], version: str, db_params: dict):
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        batch = batch.persist()  # read once for the conflict check, once for scoring
        try:
            predictions = score(batch, model, features, version)
            # Executors write their own partitions; any exception fails this micro-batch.
            predictions.foreachPartition(lambda rows: write_rows(rows, db_params))
        finally:
            batch.unpersist()
        print(f"[inference_v1] batch {batch_id} written to predictions", flush=True)

    return write_batch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "engine.measurements.v1"))
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=2000)
    parser.add_argument("--allow-local", action="store_true", help="development only: accept a local[*] master")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("smartpredict-inference-v1").getOrCreate()
    master = spark.sparkContext.master
    if master.startswith("local") and not args.allow_local:
        print(f"[inference_v1] ERROR master is {master}; the contract requires spark://spark-master:7077",
              file=sys.stderr)
        return 2

    try:
        model, features = load_model(spark, args.model_version)
    except ModelContractError as error:
        print(f"[inference_v1] ERROR {error}", file=sys.stderr, flush=True)
        return 3
    print(f"[inference_v1] master={master} model={model_uri(args.model_version)} features={len(features)} "
          f"checkpoint={args.checkpoint}", flush=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")  # ignored once the checkpoint exists
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
    )
    events = parse_kafka(raw).observe(
        QUALITY_METRICS,
        F.count(F.lit(1)).alias("received"),
        F.sum(F.when(F.col("is_valid"), 0).otherwise(1)).alias("rejected"),
    )
    valid = events.where("is_valid").drop("is_valid", "raw_value")

    query = (
        valid.writeStream.queryName("inference_v1")
        .foreachBatch(make_batch_writer(model, features, args.model_version, connection_params_from_env()))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )

    def stop(signum, _frame):
        print(f"[inference_v1] signal {signum}: stopping query, checkpoint kept", flush=True)
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
            if seen <= last_seen:
                continue
            last_seen = seen
            metrics = (progress.get("observedMetrics") or {}).get(QUALITY_METRICS) or {}
            print(f"[inference_v1] {json.dumps({'batch': seen[0], 'input_rows': progress.get('numInputRows'), 'received': metrics.get('received'), 'rejected': metrics.get('rejected')})}",
                  flush=True)

    error = query.exception()
    if error is not None:
        print(f"[inference_v1] ERROR query failed: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
