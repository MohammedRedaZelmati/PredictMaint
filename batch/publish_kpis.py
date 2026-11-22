"""Publish the Gold KPI snapshot to PostgreSQL with idempotent upserts."""

import argparse
import os

GOLD_URI = "hdfs://namenode:8020/smartpredict/gold/batch_kpis/"


def main():
    import psycopg2
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-uri", default=GOLD_URI)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smartpredict-publish-kpis").getOrCreate()
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError("Submit publish_kpis.py to spark://spark-master:7077")
    gold = spark.read.parquet(args.gold_uri)
    if gold.select("snapshot_id").distinct().count() != 1:
        raise ValueError("Gold path must contain exactly one KPI snapshot")
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
                for row in gold.toLocalIterator():
                    cursor.execute(
                        """INSERT INTO batch_kpis
                           (snapshot_id, metric_name, dimension_name, dimension_value,
                            metric_value, computed_at)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           ON CONFLICT (snapshot_id, metric_name, dimension_name, dimension_value)
                           DO UPDATE SET metric_value = EXCLUDED.metric_value,
                                         computed_at = EXCLUDED.computed_at""",
                        (row.snapshot_id, row.metric_name, row.dimension_name,
                         row.dimension_value, row.metric_value, row.computed_at),
                    )
        print(f"Published KPI snapshot {gold.first()['snapshot_id']}")
    finally:
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
