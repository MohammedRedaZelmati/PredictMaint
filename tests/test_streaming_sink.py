"""Tests for the streaming jobs (A4 archive now, A5 inference sink later).

Spark tests run with a tiny local[1] session on STATIC synthetic DataFrames that imitate
Kafka rows; they check parsing/validation logic only. They are skipped when pyspark or
Java is missing (run them inside the Spark image:
`docker compose run --rm spark-client python3 -m pytest tests/test_streaming_sink.py`).
The distributed proof on spark://spark-master:7077 is a separate integration step.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None and not __import__("os").environ.get("JAVA_HOME"):
    pytest.skip("Java not available for a local Spark session", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from ingestion.dataset_reader import Measurement  # noqa: E402
from ingestion.producer import build_event, kafka_key  # noqa: E402
from streaming.events import EVENT_FIELDS, parse_kafka  # noqa: E402

RUN_ID = "10000000-0000-4000-8000-000000000001"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("smartpredict-tests")
        .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )
    yield session
    session.stop()


def synthetic_event(engine=1, cycle=5, split="test"):
    m = Measurement(engine, cycle, (0.001, -0.0002, 100.0), tuple(500.0 + i for i in range(21)))
    return build_event(m, split, RUN_ID)


def kafka_rows(spark, messages):
    """messages: list of (key, dict-or-raw-string) -> DataFrame shaped like the Kafka source."""
    rows = []
    for offset, (key, value) in enumerate(messages):
        payload = value if isinstance(value, str) else json.dumps(value)
        rows.append((key.encode() if key else None, payload.encode(), "engine.measurements.v1", 0, offset,
                     datetime(2026, 11, 21, 12, 0, 0)))
    return spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")


def test_valid_event_is_parsed_with_provenance(spark):
    event = synthetic_event()
    out = parse_kafka(kafka_rows(spark, [(kafka_key("test", 1), event)])).collect()[0]
    assert out.is_valid
    assert out.event_id == "fd001:test:1:5" and out.engine_id == 1 and out.cycle == 5
    assert out.sensor_21 == event["sensor_21"]
    assert out.event_ts == datetime(2026, 1, 1, 0, 5, 0)
    assert (out.kafka_partition, out.kafka_offset) == (0, 0)


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(sensor_03=None),
    lambda e: e.update(cycle=0),
    lambda e: e.update(engine_id=-2),
    lambda e: e.update(source_split="valid"),
    lambda e: e.update(schema_version=2),
    lambda e: e.update(event_id="fd001:test:1:999"),
    lambda e: e.update(run_id="nope"),
    lambda e: e.update(event_ts="yesterday"),
    lambda e: e.update(target_rul=42.0),
    lambda e: e.pop("op_setting_2"),
])
def test_invalid_events_are_flagged(spark, mutate):
    event = synthetic_event()
    mutate(event)
    out = parse_kafka(kafka_rows(spark, [(kafka_key("test", 1), event)])).collect()[0]
    assert out.is_valid is False


def test_wrong_key_and_garbage_payload_are_flagged(spark):
    rows = parse_kafka(kafka_rows(spark, [
        (kafka_key("test", 2), synthetic_event(engine=1)),  # key of another engine
        (kafka_key("test", 1), "{not json"),
        (None, synthetic_event()),
    ])).collect()
    assert [r.is_valid for r in rows] == [False, False, False]


def test_schema_matches_reda_contract_when_present():
    path = Path("contracts/event.schema.json")
    if not path.is_file():
        pytest.skip("contracts/event.schema.json not merged yet")
    contract = json.loads(path.read_text(encoding="utf-8"))
    assert set(contract.get("properties", {})) == set(EVENT_FIELDS)
