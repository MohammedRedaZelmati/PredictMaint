"""FD001 Spark ML training, grouped validation and versioned pipeline export."""

import argparse
import csv
import hashlib
import io
import itertools
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path


HDFS = "hdfs://namenode:8020/smartpredict"
SILVER_URI = f"{HDFS}/silver/train_labeled/"
MODEL_URI = f"{HDFS}/models/rul_pipeline/v001/"
METADATA_URI = f"{HDFS}/models/metadata/v001/"
EXPECTED_FEATURES = ["cycle", *(f"op_setting_{i}" for i in range(1, 4))] + [
    f"sensor_{i:02d}" for i in range(1, 22)
]
TRAIN_DATASET_SHA256 = "963b5e22825b34d8b21c69e1aeb4af3e647050eb672ee8834ba4b5d91d2de0f8"


def split_engines(engine_ids, seed=42):
    provided = list(engine_ids)
    ids = sorted(set(provided))
    if len(ids) != 100 or len(provided) != 100:
        raise ValueError("Exactly 100 distinct train engines are required")
    random.Random(seed).shuffle(ids)
    return set(ids[:80]), set(ids[80:])


def snapshot_cycles(last_cycle):
    if last_cycle < 2:
        raise ValueError("A trajectory needs at least two cycles")
    return tuple(int(fraction * last_cycle) for fraction in (0.60, 0.75, 0.90))


def feature_contract():
    path = Path(__file__).resolve().parents[1] / "contracts" / "feature_columns.json"
    contract = json.loads(path.read_text(encoding="utf-8"))
    if contract != {"schema_version": 1, "features": EXPECTED_FEATURES}:
        raise ValueError("Feature contract does not match FD001 V1")
    canonical = json.dumps(EXPECTED_FEATURES, ensure_ascii=False, separators=(",", ":"))
    return EXPECTED_FEATURES, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def grouped_validation_data(silver):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ids = [row.engine_id for row in silver.select("engine_id").distinct().collect()]
    fit_ids, validation_ids = split_engines(ids)
    fit = silver.filter(F.col("engine_id").isin(sorted(fit_ids)))
    validation = silver.filter(F.col("engine_id").isin(sorted(validation_ids)))
    window = Window.partitionBy("engine_id")
    validation = validation.withColumn("last_cycle", F.max("cycle").over(window))
    positions = [F.floor(F.lit(fraction) * F.col("last_cycle")) for fraction in (0.60, 0.75, 0.90)]
    snapshots = validation.filter(
        (F.col("cycle") == positions[0])
        | (F.col("cycle") == positions[1])
        | (F.col("cycle") == positions[2])
    ).drop("last_cycle")
    if snapshots.count() != 60:
        raise ValueError("Expected three causal validation snapshots per engine")
    return fit, validation.drop("last_cycle"), snapshots


def make_pipeline(regressor, features):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Imputer, VectorAssembler

    imputed = [f"imputed_{name}" for name in features]
    return Pipeline(stages=[
        Imputer(inputCols=features, outputCols=imputed),
        VectorAssembler(inputCols=imputed, outputCol="features"),
        regressor,
    ])


def evaluate_predictions(predictions):
    from pyspark.ml.evaluation import RegressionEvaluator

    evaluator = RegressionEvaluator(labelCol="target_rul", predictionCol="prediction")
    return {
        "rmse": evaluator.setMetricName("rmse").evaluate(predictions),
        "mae": evaluator.setMetricName("mae").evaluate(predictions),
    }


def write_hdfs_text(spark, uri, content, overwrite=False):
    """Write one UTF-8 metadata file through the cluster Hadoop filesystem."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(path, overwrite)
    try:
        # py4j returns java.lang.String as a Python str (no getBytes); a bytearray maps to byte[].
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def train_and_export(spark, silver, version):
    from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
    from pyspark.sql import functions as F

    if version != "v001":
        raise ValueError("Only the contract model version v001 is supported")
    features, feature_hash = feature_contract()
    if silver.count() != 20631 or silver.select("engine_id").distinct().count() != 100:
        raise ValueError("Complete FD001 Silver train is required")
    if silver.filter(F.col("source_split") != "train").limit(1).count():
        raise ValueError("Official test observations must not enter training")
    if any(name not in silver.columns for name in [*features, "engine_id", "target_rul"]):
        raise ValueError("Silver is missing ML inputs or labels")

    fit, validation, snapshots = grouped_validation_data(silver)
    median = fit.agg(F.percentile_approx("target_rul", 0.5, 10000).alias("median")).first()["median"]
    baseline = evaluate_predictions(snapshots.withColumn("prediction", F.lit(float(median))))
    comparison = [{"candidate": "B0", "algorithm": "Median", "params": json.dumps({"median": median}),
                   "rmse": baseline["rmse"], "mae": baseline["mae"], "fit_seconds": 0.0}]

    candidates = [
        ("M1", "LinearRegression", {"regParam": 0.1}),
        ("M2", "RandomForestRegressor", {"numTrees": 50, "maxDepth": 8}),
    ]
    for depth, iterations, step in itertools.product((3, 5), (30, 60), (0.05, 0.10)):
        candidates.append(("M3", "GBTRegressor", {
            "maxDepth": depth, "maxIter": iterations, "stepSize": step,
        }))

    def regressor_for(algorithm, params):
        common = {"featuresCol": "features", "labelCol": "target_rul"}
        if algorithm == "LinearRegression":
            return LinearRegression(**common, **params)
        if algorithm == "RandomForestRegressor":
            return RandomForestRegressor(**common, seed=42, **params)
        return GBTRegressor(**common, seed=42, **params)

    best = None
    for candidate, algorithm, params in candidates:
        start = time.monotonic()
        pipeline = make_pipeline(regressor_for(algorithm, params), features)
        model = pipeline.fit(fit)
        metrics = evaluate_predictions(model.transform(snapshots))
        elapsed = time.monotonic() - start
        record = {"candidate": candidate, "algorithm": algorithm,
                  "params": json.dumps(params, sort_keys=True),
                  "rmse": metrics["rmse"], "mae": metrics["mae"], "fit_seconds": elapsed}
        comparison.append(record)
        print(f"{candidate} {algorithm} {params}: {metrics} in {elapsed:.1f}s", flush=True)
        if best is None or record["rmse"] < best["rmse"]:
            best = record
        del model

    selected_params = json.loads(best["params"])
    final_pipeline = make_pipeline(regressor_for(best["algorithm"], selected_params), features)
    final_model = final_pipeline.fit(silver)
    model_uri = MODEL_URI
    final_model.write().save(model_uri)

    manifest = {
        "schema_version": 1,
        "model_version": version,
        "model_hdfs_uri": model_uri,
        "algorithm": best["algorithm"],
        "algorithm_params": selected_params,
        "target_name": "target_rul",
        "target_cap_cycles": 125,
        "feature_columns_uri": "contracts/feature_columns.json",
        "feature_schema_hash": feature_hash,
        "split_seed": 42,
        "validation_metric": "rmse",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "train_dataset_sha256": TRAIN_DATASET_SHA256,
    }
    metrics = {"validation_snapshots": 60, "validation_rmse": best["rmse"],
               "validation_mae": best["mae"], "test_rmse": None, "test_mae": None}
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(comparison[0]))
    writer.writeheader()
    writer.writerows(comparison)
    write_hdfs_text(spark, f"{METADATA_URI}comparison.csv", buffer.getvalue())
    write_hdfs_text(spark, f"{METADATA_URI}metrics.json", json.dumps(metrics, indent=2))
    write_hdfs_text(spark, f"{METADATA_URI}manifest.json", json.dumps(manifest, indent=2))
    return manifest, comparison


def publish_model_registry(spark):
    """Upsert the frozen model metadata after training or on a safe retry."""
    import psycopg2

    manifest = json.loads("\n".join(
        row.value for row in spark.read.text(f"{METADATA_URI}manifest.json").collect()
    ))
    metrics = json.loads("\n".join(
        row.value for row in spark.read.text(f"{METADATA_URI}metrics.json").collect()
    ))
    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO model_registry
                       (model_version, model_hdfs_uri, feature_schema_hash, algorithm,
                        validation_rmse, test_rmse)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (model_version) DO UPDATE SET
                         model_hdfs_uri = EXCLUDED.model_hdfs_uri,
                         feature_schema_hash = EXCLUDED.feature_schema_hash,
                         algorithm = EXCLUDED.algorithm,
                         validation_rmse = EXCLUDED.validation_rmse,
                         test_rmse = EXCLUDED.test_rmse""",
                    (manifest["model_version"], manifest["model_hdfs_uri"],
                     manifest["feature_schema_hash"], manifest["algorithm"],
                     metrics["validation_rmse"], metrics.get("test_rmse")),
                )
    finally:
        connection.close()


def main():
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default="v001")
    parser.add_argument("--silver-uri", default=SILVER_URI)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--publish-registry-only", action="store_true")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smartpredict-fd001-train").getOrCreate()
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError("Submit train_models.py to spark://spark-master:7077")
    if args.publish_registry_only:
        publish_model_registry(spark)
        print("Published model v001 metadata to PostgreSQL")
        spark.stop()
        return
    silver = spark.read.parquet(args.silver_uri)
    if args.verify_only:
        model = PipelineModel.load(MODEL_URI)
        prediction = model.transform(silver.limit(1)).select("prediction").first()[0]
        if not isinstance(prediction, float):
            raise ValueError("Loaded pipeline did not produce a double prediction")
        print(f"Reloaded {MODEL_URI} in a new Spark session; prediction={prediction}")
        spark.stop()
        return
    manifest, _ = train_and_export(spark, silver, args.version)
    publish_model_registry(spark)
    print(f"Saved {manifest['algorithm']} pipeline at {manifest['model_hdfs_uri']}")
    spark.stop()


if __name__ == "__main__":
    main()
