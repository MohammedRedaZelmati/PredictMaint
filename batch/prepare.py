"""Build labeled train Silver from Ayman's HDFS Bronze archive."""

import argparse

from batch.data_quality import deduplicate_bronze


BRONZE_URI = "hdfs://namenode:8020/smartpredict/bronze/engine_events/"
SILVER_URI = "hdfs://namenode:8020/smartpredict/silver/train_labeled/"
RUL_CAP = 125


def rul_for_cycle(last_train_cycle, cycle):
    if cycle < 1 or last_train_cycle < cycle:
        raise ValueError("Invalid train cycle")
    return min(RUL_CAP, last_train_cycle - cycle)


def prepare_train(bronze):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    logical = deduplicate_bronze(bronze)
    train = logical.filter(F.col("source_split") == "train")
    if train.count() != 20631 or train.select("engine_id").distinct().count() != 100:
        raise ValueError("Complete FD001 train archive is required: 20,631 cycles, 100 engines")
    trajectories = train.groupBy("engine_id").agg(
        F.min("cycle").alias("first_cycle"),
        F.max("cycle").alias("last_cycle"),
        F.count("*").alias("cycle_count"),
    )
    if trajectories.filter(
        (F.col("first_cycle") != 1) | (F.col("cycle_count") != F.col("last_cycle"))
    ).limit(1).count():
        raise ValueError("Train cycles must be consecutive from one for each engine")
    window = Window.partitionBy("engine_id")
    labeled = train.withColumn("last_train_cycle", F.max("cycle").over(window))
    labeled = labeled.withColumn(
        "target_rul",
        F.least(F.lit(RUL_CAP), F.col("last_train_cycle") - F.col("cycle")).cast("double"),
    ).drop("last_train_cycle")
    if labeled.filter((F.col("target_rul") < 0) | (F.col("target_rul") > RUL_CAP)).limit(1).count():
        raise ValueError("Invalid train RUL target")
    terminal = labeled.groupBy("engine_id").agg(F.max("cycle").alias("last_cycle"))
    bad_terminal = (
        labeled.join(terminal, "engine_id")
        .filter((F.col("cycle") == F.col("last_cycle")) & (F.col("target_rul") != 0))
    )
    if bad_terminal.limit(1).count():
        raise ValueError("Train terminal RUL must equal zero")
    return labeled


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-uri", default=BRONZE_URI)
    parser.add_argument("--silver-uri", default=SILVER_URI)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smartpredict-prepare-fd001").getOrCreate()
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError("Submit prepare.py to spark://spark-master:7077")
    bronze = spark.read.parquet(args.bronze_uri)
    silver = prepare_train(bronze)
    silver.write.mode("overwrite").parquet(args.silver_uri)
    print(f"Silver train written to {args.silver_uri}: {silver.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
