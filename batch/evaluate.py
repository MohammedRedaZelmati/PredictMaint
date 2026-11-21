"""One-time official FD001 evaluation on 100 terminal test observations."""

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path

from batch.train_models import METADATA_URI, MODEL_URI, write_hdfs_text


BRONZE_URI = "hdfs://namenode:8020/smartpredict/bronze/engine_events/"
REFERENCE_URI = "hdfs://namenode:8020/smartpredict/reference/test_rul/RUL_FD001.txt"
EXPECTED_RUL_SHA256 = "a19c8ec94931949d0485bdc35118206e9c81c4547b422efb9cf86f4ceddbceca"


def stage_official_rul(spark, local_path):
    raw = Path(local_path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != EXPECTED_RUL_SHA256:
        raise ValueError("RUL file differs from the verified FD001 source")
    labels = [int(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
    if len(labels) != 100 or min(labels) < 1:
        raise ValueError("Expected 100 positive official RUL labels")
    write_hdfs_text(spark, REFERENCE_URI, raw.decode("utf-8"))


def evaluate_official(spark, bronze):
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StructField, StructType

    from batch.data_quality import deduplicate_bronze
    from batch.train_models import evaluate_predictions

    logical = deduplicate_bronze(bronze)
    test = logical.filter(F.col("source_split") == "test")
    if test.count() != 13096 or test.select("engine_id").distinct().count() != 100:
        raise ValueError("Complete FD001 test Bronze is required")
    last = test.groupBy("engine_id").agg(F.max("cycle").alias("cycle"))
    terminal = test.join(last, ["engine_id", "cycle"])
    if terminal.count() != 100:
        raise ValueError("Expected exactly one terminal observation per test engine")

    lines = spark.read.text(REFERENCE_URI).rdd.map(lambda row: row.value.strip()).filter(bool)
    indexed = lines.zipWithIndex().map(lambda item: (int(item[1]) + 1, int(item[0])))
    schema = StructType([
        StructField("engine_id", IntegerType(), False),
        StructField("official_rul", IntegerType(), False),
    ])
    labels = spark.createDataFrame(indexed, schema)
    if labels.count() != 100 or labels.select("engine_id").distinct().count() != 100:
        raise ValueError("Expected exactly 100 official test labels")

    model = PipelineModel.load(MODEL_URI)
    predicted = model.transform(terminal).join(labels, "engine_id")
    predicted = predicted.withColumn("target_rul", F.least(F.lit(125), F.col("official_rul")).cast("double"))
    predicted = predicted.withColumn(
        "predicted_rul_cycles",
        F.greatest(F.lit(0.0), F.least(F.lit(125.0), F.col("prediction"))),
    )
    if predicted.count() != 100:
        raise ValueError("Test prediction count is not 100")
    scored = predicted.withColumnRenamed("prediction", "raw_prediction")
    scored = scored.withColumn("prediction", F.col("predicted_rul_cycles"))
    metrics = evaluate_predictions(scored)

    metadata_path = f"{METADATA_URI}metrics.json"
    existing = json.loads("\n".join(row.value for row in spark.read.text(metadata_path).collect()))
    if existing.get("test_rmse") is not None:
        raise ValueError("Official test was already evaluated for v001")
    existing.update({"test_rmse": metrics["rmse"], "test_mae": metrics["mae"],
                     "test_terminal_engines": 100})
    rows = predicted.select("engine_id", "cycle", "official_rul", "target_rul",
                            "prediction", "predicted_rul_cycles").orderBy("engine_id").collect()
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(("engine_id", "cycle", "official_rul", "target_rul",
                     "raw_prediction", "predicted_rul_cycles"))
    for row in rows:
        writer.writerow(tuple(row))
    write_hdfs_text(spark, f"{METADATA_URI}predictions_test.csv", buffer.getvalue())
    write_hdfs_text(spark, metadata_path, json.dumps(existing, indent=2), overwrite=True)
    return metrics


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-uri", default=BRONZE_URI)
    parser.add_argument("--stage-reference", type=Path)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smartpredict-fd001-official-test").getOrCreate()
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError("Submit evaluate.py to spark://spark-master:7077")
    if args.stage_reference:
        stage_official_rul(spark, args.stage_reference)
    metrics = evaluate_official(spark, spark.read.parquet(args.bronze_uri))
    print(f"Official FD001 terminal test RMSE={metrics['rmse']:.4f} MAE={metrics['mae']:.4f}")
    spark.stop()


if __name__ == "__main__":
    main()
