"""Logical count of the Bronze archive (handoff check for Reda's lot R2).

    docker compose run --rm --no-deps spark-client spark-submit --conf spark.driver.host=spark-client \
        infra/healthchecks/bronze_count.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smartpredict-bronze-count").getOrCreate()
bronze = spark.read.parquet("hdfs://namenode:8020/smartpredict/bronze/engine_events/")
bronze.createOrReplaceTempView("bronze")
print(f"[bronze] master={spark.sparkContext.master}")
print("[bronze] schema: " + ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in bronze.schema.fields))
for row in spark.sql(
    """
    SELECT source_split, COUNT(*) AS rows, COUNT(DISTINCT event_id) AS logical_rows,
           COUNT(DISTINCT engine_id) AS engines, COUNT(DISTINCT run_id) AS runs
    FROM bronze GROUP BY source_split ORDER BY source_split
    """
).collect():
    print(f"[bronze] {row.asDict()}")
spark.stop()
