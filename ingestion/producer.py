"""Replay FD001 measurements to Kafka topic engine.measurements.v1 (event contract V1).

    python -m ingestion.producer --split train|test --speed <float> [--run-id <uuid>] [--engine-ids 1,2,3]

One event = one NASA row = one engine at one cycle. Events are emitted cycle by cycle
across the selected engines (cycle 1 of every engine, then cycle 2, ...), so the order
within each engine is preserved; the Kafka key `fd001:<split>:<engine_id>` keeps every
event of an engine on the same partition. No RUL, failure cycle or label is ever sent.

--speed is the number of simulated cycles replayed per second (each cycle step sends
one event per engine still running). 0 means as fast as Kafka acknowledges.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import sys
import time
from typing import Callable, Iterable, Iterator
import uuid

from ingestion.dataset_reader import SENSOR_NAMES, SETTING_NAMES, SPLITS, Measurement, read_split, split_path

SCHEMA_VERSION = 1
DATASET = "NASA-CMAPSS-FD001"
TOPIC = "engine.measurements.v1"
# Simulated event time: fixed UTC origin + cycle x 60 s. Deterministic across replays,
# and clearly not a real NASA timestamp (the dataset has none).
DEFAULT_EVENT_ORIGIN = datetime(2026, 1, 1, tzinfo=timezone.utc)
SECONDS_PER_CYCLE = 60
CONTRACT_SCHEMA = Path("contracts/event.schema.json")  # Reda's; used when present

FORBIDDEN_FIELDS = frozenset({"target_rul", "rul", "true_rul", "max_cycle", "failure_cycle"})
NUMERIC_FIELDS = SETTING_NAMES + SENSOR_NAMES

log = logging.getLogger("ingestion.producer")


def utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def kafka_key(split: str, engine_id: int) -> str:
    return f"fd001:{split}:{engine_id}"


def event_id(split: str, engine_id: int, cycle: int) -> str:
    return f"fd001:{split}:{engine_id}:{cycle}"


def build_event(
    measurement: Measurement,
    split: str,
    run_id: str,
    event_origin: datetime = DEFAULT_EVENT_ORIGIN,
    published_at: datetime | None = None,
) -> dict:
    """Event contract V1 (00_CONTRATS_COMMUNS.md section 2), field order as in the contract."""
    event = {
        "schema_version": SCHEMA_VERSION,
        "dataset": DATASET,
        "source_split": split,
        "run_id": run_id,
        "event_id": event_id(split, measurement.engine_id, measurement.cycle),
        "engine_id": measurement.engine_id,
        "cycle": measurement.cycle,
        "event_ts": utc_iso(event_origin + timedelta(seconds=SECONDS_PER_CYCLE * measurement.cycle)),
        "published_at": utc_iso(published_at or datetime.now(timezone.utc)),
    }
    event.update(zip(SETTING_NAMES, map(float, measurement.settings)))
    event.update(zip(SENSOR_NAMES, map(float, measurement.sensors)))
    return event


def validation_errors(event: dict) -> list[str]:
    """Checks applied before publication; an invalid event is rejected, never sent."""
    errors = []
    if event.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version must be 1")
    if event.get("dataset") != DATASET:
        errors.append(f"dataset must be {DATASET}")
    if event.get("source_split") not in SPLITS:
        errors.append("source_split must be train or test")
    for name in ("engine_id", "cycle"):
        value = event.get(name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            errors.append(f"{name} must be an integer >= 1")
    try:
        uuid.UUID(str(event.get("run_id")))
    except ValueError:
        errors.append("run_id must be a UUID")
    if not errors and event.get("event_id") != event_id(event["source_split"], event["engine_id"], event["cycle"]):
        errors.append("event_id does not match fd001:<split>:<engine_id>:<cycle>")
    for name in NUMERIC_FIELDS:
        value = event.get(name)
        if not isinstance(value, float) or not math.isfinite(value):
            errors.append(f"{name} must be a finite number")
    leaked = FORBIDDEN_FIELDS.intersection(event)
    if leaked:
        errors.append(f"forbidden label fields present: {sorted(leaked)}")
    return errors


def load_contract_validator(path: Path = CONTRACT_SCHEMA) -> Callable[[dict], list[str]] | None:
    """JSON Schema validation against Reda's contracts/event.schema.json, if it exists."""
    if not path.is_file():
        return None
    import jsonschema

    # Reda's schema relies on `format: uuid` / `date-time`: enable format assertions
    # (date-time needs rfc3339-validator installed, which the Spark image provides).
    validator = jsonschema.Draft202012Validator(
        json.loads(path.read_text(encoding="utf-8")),
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )
    return lambda event: [error.message for error in validator.iter_errors(event)]


def replay_order(fleet: dict[int, list[Measurement]], engine_ids: Iterable[int] | None = None) -> Iterator[list[Measurement]]:
    """Yield, for each cycle step, the measurements of every selected engine at that cycle."""
    selected = sorted(fleet) if engine_ids is None else list(engine_ids)
    missing = [engine for engine in selected if engine not in fleet]
    if missing:
        raise ValueError(f"engine ids not in dataset: {missing}")
    longest = max(len(fleet[engine]) for engine in selected)
    for index in range(longest):
        yield [fleet[engine][index] for engine in selected if index < len(fleet[engine])]


@dataclass
class ReplayStats:
    sent: int = 0
    rejected: int = 0
    failed: int = 0
    elapsed_s: float = 0.0
    errors: list[str] = field(default_factory=list)


class KafkaSink:
    """Thin wrapper so tests can substitute an in-memory sink."""

    def __init__(self, bootstrap_servers: str, topic: str):
        from kafka import KafkaProducer

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=5,
            # One in-flight request per connection: retries cannot reorder an engine's events.
            max_in_flight_requests_per_connection=1,
            linger_ms=5,
        )

    def send(self, key: str, value: bytes, on_error: Callable[[Exception], None]) -> None:
        self.producer.send(self.topic, key=key.encode(), value=value).add_errback(on_error)

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


class StdoutSink:
    """--dry-run: print `key<TAB>json` lines instead of publishing."""

    def send(self, key: str, value: bytes, on_error: Callable[[Exception], None]) -> None:
        sys.stdout.write(f"{key}\t{value.decode()}\n")

    def close(self) -> None:
        sys.stdout.flush()


def replay(
    fleet: dict[int, list[Measurement]],
    split: str,
    sink,
    run_id: str,
    speed: float,
    engine_ids: Iterable[int] | None = None,
    event_origin: datetime = DEFAULT_EVENT_ORIGIN,
    contract_validator: Callable[[dict], list[str]] | None = None,
    limit: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> ReplayStats:
    stats = ReplayStats()
    start = clock()
    interval = 1.0 / speed if speed > 0 else 0.0

    def on_error(error: Exception) -> None:
        stats.failed += 1
        stats.errors.append(repr(error))

    try:
        for step, measurements in enumerate(replay_order(fleet, engine_ids)):
            if interval:
                # Pace against the absolute schedule so slow sends do not accumulate drift.
                delay = start + step * interval - clock()
                if delay > 0:
                    sleep(delay)
            for measurement in measurements:
                if limit is not None and stats.sent + stats.rejected >= limit:
                    return stats
                event = build_event(measurement, split, run_id, event_origin)
                problems = validation_errors(event)
                if contract_validator is not None:
                    problems += contract_validator(event)
                if problems:
                    stats.rejected += 1
                    log.warning("rejected %s: %s", event.get("event_id"), "; ".join(problems))
                    continue
                payload = json.dumps(event, allow_nan=False, separators=(",", ":")).encode()
                sink.send(kafka_key(split, measurement.engine_id), payload, on_error)
                stats.sent += 1
    finally:
        sink.close()  # flushes Kafka: delivery errors are counted before we report
        stats.elapsed_s = clock() - start
    return stats


def parse_engine_ids(text: str | None) -> list[int] | None:
    if not text:
        return None
    try:
        ids = [int(part) for part in text.split(",") if part.strip()]
    except ValueError:
        raise argparse.ArgumentTypeError("--engine-ids must be a comma-separated list of integers") from None
    if not ids or any(i < 1 for i in ids) or len(set(ids)) != len(ids):
        raise argparse.ArgumentTypeError("--engine-ids must be distinct positive integers")
    return ids


def parse_run_id(text: str) -> str:
    try:
        return str(uuid.UUID(text))
    except ValueError:
        raise argparse.ArgumentTypeError(f"--run-id must be a UUID, got {text!r}") from None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ingestion.producer", description=__doc__.splitlines()[0])
    parser.add_argument("--split", required=True, choices=SPLITS)
    parser.add_argument("--speed", required=True, type=float, help="simulated cycles per second; 0 = no pacing")
    parser.add_argument("--run-id", type=parse_run_id, help="reuse a session UUID (deliberate resume); default: new UUID")
    parser.add_argument("--engine-ids", type=parse_engine_ids, help="e.g. 1,2,3; default: all engines of the split")
    # data/raw/ is where docs/data-card.md (Reda) keeps the verified NASA files.
    parser.add_argument("--data-dir", type=Path, default=Path(os.environ.get("FD001_DATA_DIR", "data/raw")))
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", TOPIC))
    parser.add_argument("--event-origin", type=datetime.fromisoformat, default=DEFAULT_EVENT_ORIGIN,
                        help="UTC origin of simulated event_ts (default 2026-01-01T00:00:00+00:00)")
    parser.add_argument("--limit", type=int, help="stop after N events (smoke tests)")
    parser.add_argument("--dry-run", action="store_true", help="print key<TAB>event lines, do not contact Kafka")
    return parser


def main(argv: list[str] | None = None, sink_factory=None) -> int:
    args = build_parser().parse_args(argv)
    if args.speed < 0 or not math.isfinite(args.speed):
        build_parser().error("--speed must be a finite number >= 0")
    if args.limit is not None and args.limit < 0:
        build_parser().error("--limit must be >= 0")
    event_origin = args.event_origin if args.event_origin.tzinfo else args.event_origin.replace(tzinfo=timezone.utc)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)

    run_id = args.run_id or str(uuid.uuid4())
    fleet = read_split(split_path(args.split, args.data_dir))
    validator = load_contract_validator()
    log.info("run_id=%s split=%s engines=%s speed=%s topic=%s contract_schema=%s",
             run_id, args.split, args.engine_ids or "all", args.speed, args.topic,
             "yes" if validator else "not found (built-in checks only)")

    if sink_factory is not None:
        sink = sink_factory(args)
    elif args.dry_run:
        sink = StdoutSink()
    else:
        sink = KafkaSink(args.bootstrap_servers, args.topic)

    stats = replay(fleet, args.split, sink, run_id, args.speed, args.engine_ids, event_origin, validator, args.limit)
    log.info("run_id=%s sent=%d rejected=%d failed=%d elapsed=%.1fs",
             run_id, stats.sent, stats.rejected, stats.failed, stats.elapsed_s)
    for error in stats.errors[:10]:
        log.error("delivery error: %s", error)
    return 1 if stats.failed or stats.rejected else 0


if __name__ == "__main__":
    sys.exit(main())
