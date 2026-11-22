"""Validate Bronze measurements before constructing a logical FD001 snapshot."""

MEASUREMENT_COLUMNS = [f"op_setting_{index}" for index in range(1, 4)] + [
    f"sensor_{index:02d}" for index in range(1, 22)
]
KEY_COLUMNS = ["source_split", "engine_id", "cycle"]
REQUIRED_COLUMNS = KEY_COLUMNS + ["event_id"] + MEASUREMENT_COLUMNS


def deduplicate_bronze(bronze):
    """Return one measurement per split/engine/cycle or raise on invalid input."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegralType, NumericType

    missing = set(REQUIRED_COLUMNS) - set(bronze.columns)
    if missing:
        raise ValueError(f"Bronze is missing columns: {sorted(missing)}")
    for name in ("engine_id", "cycle"):
        if not isinstance(bronze.schema[name].dataType, IntegralType):
            raise ValueError(f"Bronze {name} must have an integer type")
    for name in MEASUREMENT_COLUMNS:
        if not isinstance(bronze.schema[name].dataType, NumericType):
            raise ValueError(f"Bronze {name} must have a numeric type")

    expected_id = F.concat_ws(
        ":", F.lit("fd001"), F.col("source_split"),
        F.col("engine_id"), F.col("cycle"),
    )
    invalid = (
        F.col("source_split").isNull()
        | ~F.col("source_split").isin("train", "test")
        | F.col("engine_id").isNull()
        | (F.col("engine_id") < 1)
        | F.col("cycle").isNull()
        | (F.col("cycle") < 1)
        | F.col("event_id").isNull()
        | (F.col("event_id") != expected_id)
    )
    for name in MEASUREMENT_COLUMNS:
        value = F.col(name).cast("double")
        invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
    if bronze.filter(invalid).limit(1).count():
        raise ValueError("Bronze contains invalid FD001 measurements or identities")

    values = bronze.select(
        *KEY_COLUMNS,
        *(F.col(name).cast("double").alias(name) for name in MEASUREMENT_COLUMNS),
    )
    fingerprint = F.sha2(F.to_json(F.struct(*MEASUREMENT_COLUMNS)), 256)
    conflicts = (
        values.withColumn("fingerprint", fingerprint)
        .groupBy(*KEY_COLUMNS)
        .agg(F.countDistinct("fingerprint").alias("versions"))
        .filter(F.col("versions") > 1)
    )
    if conflicts.limit(1).count():
        raise ValueError("Bronze contains conflicting measurements for a logical cycle")
    return values.dropDuplicates(KEY_COLUMNS)
