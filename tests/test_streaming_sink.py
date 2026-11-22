"""Tests for the streaming jobs: A4 archive parsing and A5 inference sink.

Pure-Python tests (writer, model contract) always run. Spark tests use a tiny local[1]
session on STATIC synthetic DataFrames shaped like Kafka rows and are skipped when pyspark
or a Spark-compatible Java (8/11/17) is missing; run them inside the Spark image:
`docker compose run --rm spark-client python3 -m pytest tests/test_streaming_sink.py`.
None of this replaces the distributed proof on spark://spark-master:7077.
All fixture values are SYNTHETIC, not NASA measurements and not a trained model.
"""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil

import pytest

from streaming import postgres_writer as pw
from streaming.model_contract import (
    EXPECTED_FEATURES, ModelContractError, check_manifest, feature_schema_hash, load_feature_columns,
    model_uri,
)

RUN_ID = "10000000-0000-4000-8000-000000000001"


# ================================================================== writer (pure Python)
@pytest.mark.parametrize("prediction, rul, status", [
    (-7.0, 0.0, "CRITICAL"), (0.0, 0.0, "CRITICAL"), (20.0, 20.0, "CRITICAL"), (20.01, 20.01, "WARNING"),
    (50.0, 50.0, "WARNING"), (50.5, 50.5, "HEALTHY"), (125.0, 125.0, "HEALTHY"), (300.0, 125.0, "HEALTHY"),
])
def test_clamp_and_health_thresholds(prediction, rul, status):
    assert pw.clamp_rul(prediction) == rul
    assert pw.health_status(pw.clamp_rul(prediction)) == status


@pytest.mark.parametrize("bad", [None, float("nan")])
def test_missing_prediction_is_never_replaced(bad):
    with pytest.raises(ValueError):
        pw.clamp_rul(bad)


def prediction_row(event_id="fd001:test:1:5", rul=42.0):
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    return {
        "run_id": RUN_ID, "event_id": event_id, "model_version": "v001", "source_split": "test",
        "engine_id": 1, "cycle": 5, "event_ts": now, "published_at": now, "predicted_at": now,
        "predicted_rul_cycles": rul, "health_status": pw.health_status(rul),
    }


def test_upsert_sql_targets_contract_key_and_columns():
    assert "INSERT INTO predictions (run_id, event_id, model_version, source_split, engine_id, cycle, " \
           "event_ts, published_at, predicted_at, predicted_rul_cycles, health_status)" in pw.UPSERT_SQL
    assert "ON CONFLICT (run_id, event_id, model_version) DO UPDATE" in pw.UPSERT_SQL
    assert pw.ROW_TEMPLATE.count("%s") == len(pw.PREDICTION_COLUMNS)


class FakeConnection:
    def __init__(self, store, fail=False):
        self.store, self.fail, self.closed = store, fail, False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        return False

    def cursor(self):
        return self

    def close(self):
        self.closed = True


def fake_db(failures_before_success=0):
    """In-memory table keyed like the SQL primary key, honouring UPSERT semantics."""
    table, calls = {}, {"connect": 0}

    def connect(params):
        calls["connect"] += 1
        if calls["connect"] <= failures_before_success:
            raise ConnectionError("postgres unavailable")
        return FakeConnection(table)

    def execute(cursor, rows, page_size):
        for row in rows:
            table[(row[0], row[1], row[2])] = row

    return table, calls, connect, execute


def test_same_event_twice_gives_one_row():
    table, _, connect, execute = fake_db()
    pw.write_rows([prediction_row()], {}, connect=connect, execute=execute)
    pw.write_rows([prediction_row(rul=41.0)], {}, connect=connect, execute=execute)
    assert len(table) == 1
    assert list(table.values())[0][9] == 41.0


def test_transient_db_error_is_retried():
    table, calls, connect, execute = fake_db(failures_before_success=2)
    assert pw.write_rows([prediction_row()], {}, connect=connect, execute=execute, sleep=lambda s: None) == 1
    assert calls["connect"] == 3 and len(table) == 1


def test_persistent_db_error_fails_the_batch():
    _, _, connect, execute = fake_db(failures_before_success=99)
    with pytest.raises(ConnectionError):
        pw.write_rows([prediction_row()], {}, attempts=3, connect=connect, execute=execute, sleep=lambda s: None)


def test_row_with_missing_prediction_is_refused():
    _, calls, connect, execute = fake_db()
    with pytest.raises(ValueError, match="predicted_rul_cycles"):
        row = prediction_row() | {"predicted_rul_cycles": None, "health_status": None}
        pw.write_rows([row], {}, connect=connect, execute=execute)
    assert calls["connect"] == 0


def test_empty_partition_does_not_connect():
    _, calls, connect, execute = fake_db()
    assert pw.write_rows([], {}, connect=connect, execute=execute) == 0 and calls["connect"] == 0


def test_db_params_come_from_environment():
    params = pw.connection_params_from_env({"POSTGRES_HOST": "db", "POSTGRES_PORT": "6543",
                                            "POSTGRES_PASSWORD": "from-env"})
    assert (params["host"], params["port"], params["password"]) == ("db", 6543, "from-env")


# ================================================================== model contract (pure Python)
def good_manifest(version="v001"):
    return {
        "schema_version": 1, "model_version": version, "model_hdfs_uri": model_uri(version),
        "algorithm": "synthetic-test-value", "target_name": "target_rul", "target_cap_cycles": 125,
        "feature_columns_uri": "contracts/feature_columns.json",
        "feature_schema_hash": feature_schema_hash(list(EXPECTED_FEATURES)),
    }


def test_feature_hash_uses_contract_serialisation():
    canonical = json.dumps(list(EXPECTED_FEATURES), ensure_ascii=False, separators=(",", ":"))
    assert len(EXPECTED_FEATURES) == 25 and canonical.startswith('["cycle","op_setting_1"')
    import hashlib
    assert feature_schema_hash(list(EXPECTED_FEATURES)) == hashlib.sha256(canonical.encode()).hexdigest()


def test_valid_manifest_is_accepted():
    check_manifest(good_manifest(), "v001", list(EXPECTED_FEATURES))
    manifest = good_manifest() | {"model_hdfs_uri": model_uri("v001").rstrip("/"),
                                  "feature_schema_hash": "sha256:" + feature_schema_hash(list(EXPECTED_FEATURES))}
    check_manifest(manifest, "v001", list(EXPECTED_FEATURES))


@pytest.mark.parametrize("change", [
    {"schema_version": 2}, {"model_version": "v002"}, {"target_cap_cycles": 130}, {"target_name": "rul"},
    {"model_hdfs_uri": "hdfs://namenode:8020/smartpredict/models/rul_pipeline/v002/"},
    {"feature_schema_hash": "0" * 64},
])
def test_incompatible_manifest_is_rejected(change):
    with pytest.raises(ModelContractError):
        check_manifest(good_manifest() | change, "v001", list(EXPECTED_FEATURES))


def test_feature_columns_file_checks(tmp_path):
    with pytest.raises(ModelContractError, match="missing"):
        load_feature_columns(tmp_path / "absent.json")
    reordered = tmp_path / "reordered.json"
    reordered.write_text(json.dumps({"schema_version": 1, "features": list(reversed(EXPECTED_FEATURES))}))
    with pytest.raises(ModelContractError, match="strict order"):
        load_feature_columns(reordered)
    good = tmp_path / "good.json"
    good.write_text(json.dumps({"schema_version": 1, "features": list(EXPECTED_FEATURES)}))
    assert load_feature_columns(good) == list(EXPECTED_FEATURES)


@pytest.mark.skipif(not Path("contracts/feature_columns.json").is_file(), reason="Reda's contracts not merged yet")
def test_reda_feature_columns_match_contract():
    assert load_feature_columns() == list(EXPECTED_FEATURES)


# ================================================================== Spark (optional locally)
def _spark_usable() -> bool:
    if importlib.util.find_spec("pyspark") is None:
        return False
    java_home = os.environ.get("JAVA_HOME", "")
    # Spark 3.5 supports Java 8/11/17 only; the image provides 17.
    return any(v in java_home for v in ("17", "11", "1.8")) or (not java_home and shutil.which("java") and
                                                               os.path.exists("/opt/spark"))


requires_spark = pytest.mark.skipif(not _spark_usable(), reason="pyspark + Java 8/11/17 not available here")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]").appName("smartpredict-tests")
        .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )
    yield session
    session.stop()


def synthetic_event(engine=1, cycle=5, split="test"):
    from ingestion.dataset_reader import Measurement
    from ingestion.producer import build_event

    m = Measurement(engine, cycle, (0.001, -0.0002, 100.0), tuple(500.0 + i for i in range(21)))
    return build_event(m, split, RUN_ID)


def kafka_rows(spark, messages):
    rows = []
    for offset, (key, value) in enumerate(messages):
        payload = value if isinstance(value, str) else json.dumps(value)
        rows.append((key.encode() if key else None, payload.encode(), "engine.measurements.v1", 0, offset,
                     datetime(2026, 11, 21, 12, 0, 0)))
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")


@requires_spark
def test_valid_event_is_parsed_with_provenance(spark):
    from streaming.events import parse_kafka

    event = synthetic_event()
    out = parse_kafka(kafka_rows(spark, [("fd001:test:1", event)])).collect()[0]
    assert out.is_valid
    assert (out.event_id, out.engine_id, out.cycle) == ("fd001:test:1:5", 1, 5)
    assert out.event_ts == datetime(2026, 1, 1, 0, 5, 0)


@requires_spark
@pytest.mark.parametrize("mutate", [
    lambda e: e.update(sensor_03=None), lambda e: e.update(cycle=0), lambda e: e.update(engine_id=-2),
    lambda e: e.update(source_split="valid"), lambda e: e.update(schema_version=2),
    lambda e: e.update(event_id="fd001:test:1:999"), lambda e: e.update(run_id="nope"),
    lambda e: e.update(event_ts="yesterday"), lambda e: e.update(target_rul=42.0),
    lambda e: e.pop("op_setting_2"),
    # Review on #7: keys outside the contract, even with a null value, must be rejected.
    lambda e: e.update(debug="x"),
    lambda e: e.update(target_rul=None),
    lambda e: e.update(rul=None),
    # Review on #7: the contract requires UTC; to_timestamp() alone would accept an offset.
    lambda e: e.update(event_ts="2026-01-01T01:00:00+01:00"),
    lambda e: e.update(published_at="2026-11-21T13:00:00.000+01:00"),
    lambda e: e.update(event_ts="2026-01-01T00:05:00"),
    lambda e: e.update(published_at="2026-11-21 12:00:00Z"),
    lambda e: e.update(event_ts="2026-13-01T00:05:00Z"),
])
def test_invalid_events_are_flagged(spark, mutate):
    from streaming.events import parse_kafka

    event = synthetic_event()
    mutate(event)
    assert parse_kafka(kafka_rows(spark, [("fd001:test:1", event)])).collect()[0].is_valid is False


@requires_spark
@pytest.mark.parametrize("event_ts, published_at", [
    ("2026-01-01T00:05:00Z", "2026-11-21T12:00:00.123Z"),
    ("2026-01-01T00:05:00+00:00", "2026-11-21T12:00:00.123456+00:00"),
])
def test_utc_timestamps_are_accepted(spark, event_ts, published_at):
    from streaming.events import parse_kafka

    event = synthetic_event()
    event.update(event_ts=event_ts, published_at=published_at)
    out = parse_kafka(kafka_rows(spark, [("fd001:test:1", event)])).collect()[0]
    assert out.is_valid
    assert out.event_ts == datetime(2026, 1, 1, 0, 5, 0)
    assert "_event_ts_is_utc" not in out.asDict()


@requires_spark
def test_wrong_key_and_garbage_payload_are_flagged(spark):
    from streaming.events import parse_kafka

    rows = parse_kafka(kafka_rows(spark, [
        ("fd001:test:2", synthetic_event(engine=1)), ("fd001:test:1", "{not json"), (None, synthetic_event()),
    ])).collect()
    assert [r.is_valid for r in rows] == [False, False, False]


@requires_spark
def test_score_clamps_and_dedups_with_a_synthetic_pipeline(spark):
    """Uses a throwaway LinearRegression fitted on synthetic rows, NOT Reda's model."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from streaming.events import parse_kafka
    from streaming.inference import score

    features = list(EXPECTED_FEATURES)
    train = spark.createDataFrame([tuple(float(c) for _ in features) + (130.0 - c * 10,) for c in range(1, 6)],
                                  features + ["target_rul"])
    model = Pipeline(stages=[VectorAssembler(inputCols=features, outputCol="features"),
                             LinearRegression(labelCol="target_rul", maxIter=5)]).fit(train)
    events = [synthetic_event(cycle=c) for c in (1, 1, 2)]  # duplicate event in one batch
    batch = parse_kafka(kafka_rows(spark, [("fd001:test:1", e) for e in events])).where("is_valid")
    rows = score(batch.drop("is_valid", "raw_value"), model, features, "v001").collect()
    assert len(rows) == 2
    assert all(0.0 <= r.predicted_rul_cycles <= 125.0 for r in rows)
    assert all(r.health_status == pw.health_status(r.predicted_rul_cycles) for r in rows)
    assert list(rows[0].asDict()) == list(pw.PREDICTION_COLUMNS)


def test_schema_matches_reda_contract_when_present():
    path = Path("contracts/event.schema.json")
    if not path.is_file():
        pytest.skip("contracts/event.schema.json not merged yet")
    if importlib.util.find_spec("pyspark") is None:
        pytest.skip("pyspark not installed")
    from streaming.events import EVENT_FIELDS

    assert set(json.loads(path.read_text(encoding="utf-8")).get("properties", {})) == set(EVENT_FIELDS)
