"""Compute six FD001 KPI families with Spark SQL on logical HDFS data."""

import argparse

from batch.data_quality import MEASUREMENT_COLUMNS, deduplicate_bronze


BRONZE_URI = "hdfs://namenode:8020/smartpredict/bronze/engine_events/"
SILVER_URI = "hdfs://namenode:8020/smartpredict/silver/train_labeled/"
GOLD_URI = "hdfs://namenode:8020/smartpredict/gold/batch_kpis/"


def compute_kpis(spark, bronze, silver):
    from pyspark.sql import functions as F

    logical = deduplicate_bronze(bronze)
    bronze.createOrReplaceTempView("fd001_bronze")
    logical.createOrReplaceTempView("fd001_logical")
    silver.createOrReplaceTempView("fd001_silver")

    # The snapshot ID is a content digest of all deduplicated observations.
    columns = ["source_split", "engine_id", "cycle", *MEASUREMENT_COLUMNS]
    row_digest = F.sha2(F.to_json(F.struct(*columns)), 256)
    snapshot_id = logical.select(row_digest.alias("digest")).agg(
        F.sha2(F.concat_ws("", F.sort_array(F.collect_list("digest"))), 256).alias("id")
    ).first()["id"]

    queries = [
        """SELECT 'measurements' metric_name, 'split' dimension_name,
                  source_split dimension_value, CAST(COUNT(*) AS DOUBLE) metric_value
           FROM fd001_logical GROUP BY source_split""",
        """SELECT 'engines' metric_name, 'split' dimension_name,
                  source_split dimension_value,
                  CAST(COUNT(DISTINCT engine_id) AS DOUBLE) metric_value
           FROM fd001_logical GROUP BY source_split""",
        """WITH lengths AS (
             SELECT engine_id, COUNT(*) length FROM fd001_logical
             WHERE source_split='train' GROUP BY engine_id
           )
           SELECT 'trajectory_min_cycles' metric_name, 'all' dimension_name,
                  'all' dimension_value, CAST(MIN(length) AS DOUBLE) metric_value FROM lengths
           UNION ALL
           SELECT 'trajectory_mean_cycles', 'all', 'all', AVG(length) FROM lengths
           UNION ALL
           SELECT 'trajectory_median_cycles', 'all', 'all',
                  CAST(percentile_approx(length, 0.5) AS DOUBLE) FROM lengths
           UNION ALL
           SELECT 'trajectory_max_cycles', 'all', 'all', CAST(MAX(length) AS DOUBLE) FROM lengths""",
        """SELECT 'quality_error_count' metric_name, 'all' dimension_name,
                  'all' dimension_value,
                  CAST((SELECT COUNT(*) FROM fd001_bronze) -
                       (SELECT COUNT(*) FROM fd001_logical) AS DOUBLE) metric_value""",
        """SELECT 'train_cycles_by_rul_band' metric_name, 'rul_band' dimension_name,
                  CASE WHEN target_rul <= 20 THEN '0-20'
                       WHEN target_rul <= 50 THEN '21-50'
                       ELSE '51-125' END dimension_value,
                  CAST(COUNT(*) AS DOUBLE) metric_value
           FROM fd001_silver GROUP BY
                  CASE WHEN target_rul <= 20 THEN '0-20'
                       WHEN target_rul <= 50 THEN '21-50'
                       ELSE '51-125' END""",
    ]
    missing_terms = " + ".join(
        f"SUM(CASE WHEN {name} IS NULL THEN 1 ELSE 0 END)" for name in MEASUREMENT_COLUMNS
    )
    queries.append(
        f"""SELECT 'missing_value_pct' metric_name, 'all' dimension_name,
                   'all' dimension_value,
                   CAST(100.0 * ({missing_terms}) / (COUNT(*) * {len(MEASUREMENT_COLUMNS)}) AS DOUBLE) metric_value
            FROM fd001_bronze"""
    )
    # These five FD001 sensors vary across trajectories; include split in each band.
    for sensor in ("sensor_02", "sensor_04", "sensor_07", "sensor_11", "sensor_12"):
        queries.append(f"""SELECT '{sensor}_mean' metric_name, 'split_cycle_band' dimension_name,
                      CONCAT(source_split, ':', CAST(FLOOR((cycle-1)/50)*50+1 AS STRING)) dimension_value,
                      AVG({sensor}) metric_value
               FROM fd001_logical GROUP BY source_split, FLOOR((cycle-1)/50)
               UNION ALL
               SELECT '{sensor}_stddev', 'split_cycle_band',
                      CONCAT(source_split, ':', CAST(FLOOR((cycle-1)/50)*50+1 AS STRING)),
                      stddev_pop({sensor})
               FROM fd001_logical GROUP BY source_split, FLOOR((cycle-1)/50)""")

    frames = [spark.sql(query) for query in queries]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result.withColumn("snapshot_id", F.lit(snapshot_id)).withColumn(
        "computed_at", F.current_timestamp()
    ).select("snapshot_id", "metric_name", "dimension_name", "dimension_value", "metric_value", "computed_at")


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-uri", default=BRONZE_URI)
    parser.add_argument("--silver-uri", default=SILVER_URI)
    parser.add_argument("--gold-uri", default=GOLD_URI)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smartpredict-fd001-kpis").getOrCreate()
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError("Submit kpis.py to spark://spark-master:7077")
    bronze = spark.read.parquet(args.bronze_uri)
    silver = spark.read.parquet(args.silver_uri)
    kpis = compute_kpis(spark, bronze, silver)
    kpis.write.mode("overwrite").parquet(args.gold_uri)
    print(f"Gold KPI snapshot written to {args.gold_uri}: {kpis.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
