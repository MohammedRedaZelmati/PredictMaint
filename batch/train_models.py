"""FD001 Spark ML training, grouped validation and versioned pipeline export."""

import argparse
import hashlib
import json
import random
from pathlib import Path


HDFS = "hdfs://namenode:8020/smartpredict"
SILVER_URI = f"{HDFS}/silver/train_labeled/"
MODEL_URI = f"{HDFS}/models/rul_pipeline/v001/"
METADATA_URI = f"{HDFS}/models/metadata/v001/"
EXPECTED_FEATURES = ["cycle", *(f"op_setting_{i}" for i in range(1, 4))] + [
    f"sensor_{i:02d}" for i in range(1, 22)
]


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
