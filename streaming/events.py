"""Spark-side parsing and validation of event contract V1, shared by archive and inference.

The schema mirrors docs/guides/00_CONTRATS_COMMUNS.md section 2. When Reda's
contracts/event.schema.json is merged, tests/test_streaming_sink.py checks that both
list the same fields.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

SETTING_NAMES = tuple(f"op_setting_{i}" for i in range(1, 4))
SENSOR_NAMES = tuple(f"sensor_{i:02d}" for i in range(1, 22))
NUMERIC_FIELDS = SETTING_NAMES + SENSOR_NAMES
FORBIDDEN_FIELDS = ("target_rul", "rul", "true_rul", "max_cycle", "failure_cycle")

EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("dataset", StringType()),
        StructField("source_split", StringType()),
        StructField("run_id", StringType()),
        StructField("event_id", StringType()),
        StructField("engine_id", IntegerType()),
        StructField("cycle", IntegerType()),
        StructField("event_ts", StringType()),
        StructField("published_at", StringType()),
    ]
    + [StructField(name, DoubleType()) for name in NUMERIC_FIELDS]
)
EVENT_FIELDS = tuple(field.name for field in EVENT_SCHEMA.fields)

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def _finite(column: Column) -> Column:
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) < F.lit(float("inf")))


def parse_kafka(raw: DataFrame) -> DataFrame:
    """Kafka rows -> one column per contract field + Kafka provenance + `is_valid`.

    Invalid messages are kept (flagged) so callers can count them before filtering.
    """
    value = F.col("value").cast("string")
    parsed = raw.select(
        F.from_json(value, EVENT_SCHEMA).alias("e"),
        value.alias("raw_value"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    events = parsed.select(
        *[F.col(f"e.{name}").alias(name) for name in EVENT_FIELDS],
        "raw_value", "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    )
    events = events.withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
        "published_at", F.to_timestamp("published_at")
    )

    checks = [
        F.col("schema_version") == 1,
        F.col("dataset") == "NASA-CMAPSS-FD001",
        F.col("source_split").isin("train", "test"),
        F.col("run_id").rlike(UUID_PATTERN),
        F.col("engine_id") >= 1,
        F.col("cycle") >= 1,
        F.col("event_id") == F.concat_ws(":", F.lit("fd001"), "source_split", "engine_id", "cycle"),
        F.col("kafka_key") == F.concat_ws(":", F.lit("fd001"), "source_split", "engine_id"),
        F.col("event_ts").isNotNull(),
        F.col("published_at").isNotNull(),
    ]
    checks += [_finite(F.col(name)) for name in NUMERIC_FIELDS]
    # A label leaking into Kafka is a contract breach even if every required field is fine.
    checks += [F.get_json_object("raw_value", f"$.{name}").isNull() for name in FORBIDDEN_FIELDS]

    is_valid = checks[0]
    for check in checks[1:]:
        is_valid = is_valid & check
    return events.withColumn("is_valid", F.coalesce(is_valid, F.lit(False)))
