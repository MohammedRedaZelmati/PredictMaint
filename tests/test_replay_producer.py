"""Tests for the FD001 reader and Kafka replay producer (lot A3).

All fixture rows below are SYNTHETIC test data, not NASA measurements. Tests that need
the real FD001 files are skipped unless data/train_FD001.txt / data/test_FD001.txt exist.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import uuid

import pytest

from ingestion import producer
from ingestion.dataset_reader import DatasetError, Measurement, read_split, split_path, summary

DATA_DIR = Path("data")
RUN_ID = "10000000-0000-4000-8000-000000000001"


def synthetic_row(engine: int, cycle: int, offset: float = 0.0) -> str:
    settings = [0.001 * cycle, -0.0002, 100.0]
    sensors = [500.0 + i + offset + 0.01 * cycle for i in range(21)]
    return " ".join(str(v) for v in [engine, cycle, *settings, *sensors])


def write_fixture(tmp_path: Path, rows: list[str], name: str = "test_FD001.txt") -> Path:
    path = tmp_path / name
    path.write_text("\n".join(rows) + "\n", encoding="ascii")
    return path


@pytest.fixture
def fleet(tmp_path):
    # Engine 1: 3 cycles, engine 2: 2 cycles, engine 3: 4 cycles; rows deliberately shuffled.
    rows = [synthetic_row(2, 2), synthetic_row(1, 1), synthetic_row(3, 1), synthetic_row(1, 3),
            synthetic_row(3, 2), synthetic_row(2, 1), synthetic_row(1, 2), synthetic_row(3, 3),
            synthetic_row(3, 4)]
    return read_split(write_fixture(tmp_path, rows))


class ListSink:
    def __init__(self):
        self.records: list[tuple[str, dict]] = []
        self.closed = False

    def send(self, key, value, on_error):
        self.records.append((key, json.loads(value)))

    def close(self):
        self.closed = True


class FailingSink(ListSink):
    def send(self, key, value, on_error):
        on_error(RuntimeError("broker unavailable"))


# ------------------------------------------------------------------ reader
def test_reader_groups_engines_in_cycle_order(fleet):
    assert list(fleet) == [1, 2, 3]
    assert [m.cycle for m in fleet[3]] == [1, 2, 3, 4]
    assert summary(fleet) == {"engines": 3, "rows": 9, "min_cycles": 2, "max_cycles": 4}
    assert len(fleet[1][0].settings) == 3 and len(fleet[1][0].sensors) == 21


@pytest.mark.parametrize("bad_row, message", [
    ("1 1 " + " ".join(["0.5"] * 23), "expected 26 columns"),
    ("1 1 " + " ".join(["0.5"] * 25), "expected 26 columns"),
    ("1 1 nan " + " ".join(["0.5"] * 23), "non-finite"),
    ("1 1 abc " + " ".join(["0.5"] * 23), "non-numeric"),
    ("0 1 " + " ".join(["0.5"] * 24), "engine id"),
    ("1.5 1 " + " ".join(["0.5"] * 24), "engine id"),
    ("1 0 " + " ".join(["0.5"] * 24), "cycle"),
])
def test_reader_rejects_malformed_rows(tmp_path, bad_row, message):
    with pytest.raises(DatasetError, match=message):
        read_split(write_fixture(tmp_path, [bad_row]))


@pytest.mark.parametrize("cycles", [[1, 3], [1, 1, 2], [2, 3]])
def test_reader_rejects_gaps_and_duplicates(tmp_path, cycles):
    with pytest.raises(DatasetError, match="missing or duplicate"):
        read_split(write_fixture(tmp_path, [synthetic_row(1, c) for c in cycles]))


def test_reader_rejects_empty_file(tmp_path):
    with pytest.raises(DatasetError, match="no measurements"):
        read_split(write_fixture(tmp_path, [""]))


def test_split_paths_never_point_at_rul_file():
    assert split_path("train", DATA_DIR).name == "train_FD001.txt"
    assert split_path("test", DATA_DIR).name == "test_FD001.txt"
    with pytest.raises(ValueError):
        split_path("rul", DATA_DIR)


# ------------------------------------------------------------------ event contract
def test_event_has_every_contract_field_and_no_label(fleet):
    event = producer.build_event(fleet[1][1], "test", RUN_ID)
    expected = ["schema_version", "dataset", "source_split", "run_id", "event_id", "engine_id", "cycle",
                "event_ts", "published_at", "op_setting_1", "op_setting_2", "op_setting_3",
                *[f"sensor_{i:02d}" for i in range(1, 22)]]
    assert list(event) == expected
    assert event["schema_version"] == 1 and event["dataset"] == "NASA-CMAPSS-FD001"
    assert event["event_id"] == "fd001:test:1:2"
    assert not {"target_rul", "rul", "max_cycle"} & set(event)
    assert producer.validation_errors(event) == []
    json.dumps(event, allow_nan=False)


def test_event_ts_is_simulated_from_fixed_origin(fleet):
    event = producer.build_event(fleet[1][2], "train", RUN_ID)  # cycle 3
    assert event["event_ts"] == "2026-01-01T00:03:00.000Z"
    origin = datetime(2030, 5, 1, tzinfo=timezone.utc)
    assert producer.build_event(fleet[1][0], "train", RUN_ID, origin)["event_ts"] == "2030-05-01T00:01:00.000Z"
    assert producer.build_event(fleet[1][0], "train", RUN_ID)["published_at"].endswith("Z")


@pytest.mark.parametrize("field, value", [
    ("sensor_05", math.nan), ("op_setting_1", math.inf), ("cycle", 0), ("engine_id", -1),
    ("engine_id", True), ("source_split", "valid"), ("run_id", "not-a-uuid"), ("schema_version", 2),
    ("event_id", "fd001:test:1:999"), ("target_rul", 42),
])
def test_invalid_events_are_detected(fleet, field, value):
    event = producer.build_event(fleet[1][1], "test", RUN_ID)
    event[field] = value
    assert producer.validation_errors(event)


def test_key_and_event_id_formats():
    assert producer.kafka_key("train", 7) == "fd001:train:7"
    assert producer.event_id("test", 7, 31) == "fd001:test:7:31"


# ------------------------------------------------------------------ replay
def test_replay_keeps_per_engine_order_and_key(fleet):
    sink = ListSink()
    stats = producer.replay(fleet, "test", sink, RUN_ID, speed=0)
    assert (stats.sent, stats.rejected, stats.failed) == (9, 0, 0) and sink.closed
    for engine in (1, 2, 3):
        mine = [(k, e) for k, e in sink.records if e["engine_id"] == engine]
        assert {k for k, _ in mine} == {f"fd001:test:{engine}"}
        assert [e["cycle"] for _, e in mine] == list(range(1, len(fleet[engine]) + 1))
    # Cycle-major interleaving: every engine's cycle 1 precedes any cycle 2.
    assert [e["cycle"] for _, e in sink.records][:3] == [1, 1, 1]
    assert {e["run_id"] for _, e in sink.records} == {RUN_ID}


def test_engine_filter_and_unknown_engine(fleet):
    sink = ListSink()
    producer.replay(fleet, "test", sink, RUN_ID, speed=0, engine_ids=[3, 1])
    assert {e["engine_id"] for _, e in sink.records} == {1, 3}
    assert len(sink.records) == 7
    with pytest.raises(ValueError, match="not in dataset"):
        producer.replay(fleet, "test", ListSink(), RUN_ID, speed=0, engine_ids=[9])


def test_event_ids_stable_across_replays_run_ids_differ(fleet, tmp_path):
    first, second = ListSink(), ListSink()
    producer.main(["--split", "test", "--speed", "0", "--data-dir", str(tmp_path)], sink_factory=lambda a: first)
    producer.main(["--split", "test", "--speed", "0", "--data-dir", str(tmp_path)], sink_factory=lambda a: second)
    assert [e["event_id"] for _, e in first.records] == [e["event_id"] for _, e in second.records]
    run_ids = {e["run_id"] for _, e in first.records} | {e["run_id"] for _, e in second.records}
    assert len(run_ids) == 2 and all(uuid.UUID(r) for r in run_ids)


def test_explicit_run_id_is_reused_for_deliberate_resume(fleet, tmp_path):
    sink = ListSink()
    code = producer.main(["--split", "test", "--speed", "0", "--data-dir", str(tmp_path), "--run-id", RUN_ID,
                          "--engine-ids", "2"], sink_factory=lambda a: sink)
    assert code == 0
    assert {e["run_id"] for _, e in sink.records} == {RUN_ID}
    assert [e["event_id"] for _, e in sink.records] == ["fd001:test:2:1", "fd001:test:2:2"]


def test_limit_stops_early(fleet):
    sink = ListSink()
    assert producer.replay(fleet, "test", sink, RUN_ID, speed=0, limit=4).sent == 4
    assert len(sink.records) == 4 and sink.closed


def test_speed_paces_cycle_steps():
    fleet = {1: [Measurement(1, c, (0.0, 0.0, 100.0), tuple([1.0] * 21)) for c in range(1, 5)]}
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    producer.replay(fleet, "train", ListSink(), RUN_ID, speed=2.0, sleep=sleep, clock=lambda: now[0])
    assert sleeps == [0.5, 0.5, 0.5]  # 4 cycle steps at 2 cycles/s


def test_delivery_failures_are_counted_and_fail_the_run(fleet, tmp_path):
    code = producer.main(["--split", "test", "--speed", "0", "--data-dir", str(tmp_path)],
                         sink_factory=lambda a: FailingSink())
    assert code == 1


def test_contract_validator_rejects_before_publish(fleet):
    sink = ListSink()
    stats = producer.replay(fleet, "test", sink, RUN_ID, speed=0, contract_validator=lambda e: ["synthetic rule"])
    assert stats.rejected == 9 and stats.sent == 0 and sink.records == []


@pytest.mark.parametrize("argv", [
    ["--split", "valid", "--speed", "1"],
    ["--split", "test", "--speed", "-1"],
    ["--split", "test", "--speed", "1", "--run-id", "abc"],
    ["--split", "test", "--speed", "1", "--engine-ids", "1,1"],
    ["--split", "test", "--speed", "1", "--engine-ids", "0"],
])
def test_cli_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit):
        producer.main(argv, sink_factory=lambda a: ListSink())


# ------------------------------------------------------------------ Reda's JSON Schema (when merged)
@pytest.mark.skipif(not producer.CONTRACT_SCHEMA.is_file(), reason="contracts/event.schema.json not merged yet")
def test_events_match_reda_json_schema(fleet):
    validator = producer.load_contract_validator()
    event = producer.build_event(fleet[1][0], "test", RUN_ID)
    assert validator(event) == []
    assert validator({**event, "target_rul": 10.0}) or producer.validation_errors({**event, "target_rul": 10.0})


# ------------------------------------------------------------------ real NASA files (when present)
@pytest.mark.parametrize("split, engines, rows", [("train", 100, 20631), ("test", 100, 13096)])
def test_real_fd001_counts(split, engines, rows):
    path = split_path(split, DATA_DIR)
    if not path.is_file():
        pytest.skip(f"{path} not present")
    fleet = read_split(path)
    assert summary(fleet)["engines"] == engines
    assert summary(fleet)["rows"] == rows
    sink = ListSink()
    producer.replay(fleet, split, sink, RUN_ID, speed=0, engine_ids=[1, 2, 3])
    assert all(producer.validation_errors(e) == [] for _, e in sink.records)
