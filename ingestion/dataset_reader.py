"""Strict reader for NASA C-MAPSS FD001 train/test measurement files.

Each line holds 26 whitespace-separated numbers: engine id, cycle, 3 operational
settings and 21 sensors. The terminal RUL file (RUL_FD001.txt) is deliberately not
read here: true remaining life must never reach Kafka.

Standard library only, so the producer runs in any container without numpy.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

COLUMNS = 26
SETTING_NAMES = tuple(f"op_setting_{i}" for i in range(1, 4))
SENSOR_NAMES = tuple(f"sensor_{i:02d}" for i in range(1, 22))
SPLITS = ("train", "test")
FILE_NAMES = {"train": "train_FD001.txt", "test": "test_FD001.txt"}


class DatasetError(ValueError):
    """The source file does not match the FD001 layout."""


@dataclass(frozen=True)
class Measurement:
    engine_id: int
    cycle: int
    settings: tuple[float, float, float]
    sensors: tuple[float, ...]  # 21 values, sensor_01 .. sensor_21


def split_path(split: str, data_dir: Path) -> Path:
    if split not in SPLITS:
        raise ValueError(f"split must be one of {SPLITS}, got {split!r}")
    return Path(data_dir) / FILE_NAMES[split]


def _positive_int(value: float, what: str, where: str) -> int:
    if value != math.floor(value) or value < 1:
        raise DatasetError(f"{where}: {what} must be a positive integer, got {value!r}")
    return int(value)


def parse_line(line: str, where: str) -> Measurement:
    fields = line.split()
    if len(fields) != COLUMNS:
        raise DatasetError(f"{where}: expected {COLUMNS} columns, found {len(fields)}")
    try:
        values = [float(field) for field in fields]
    except ValueError as error:
        raise DatasetError(f"{where}: non-numeric value ({error})") from None
    if not all(math.isfinite(value) for value in values):
        raise DatasetError(f"{where}: non-finite value")
    return Measurement(
        engine_id=_positive_int(values[0], "engine id", where),
        cycle=_positive_int(values[1], "cycle", where),
        settings=tuple(values[2:5]),
        sensors=tuple(values[5:]),
    )


def read_split(path: Path) -> dict[int, list[Measurement]]:
    """Return {engine_id: measurements in cycle order}, rejecting any malformed file.

    Every engine must have exactly the cycles 1..n, without gaps or duplicates.
    """
    path = Path(path)
    fleet: dict[int, list[Measurement]] = {}
    with path.open(encoding="ascii") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            measurement = parse_line(line, f"{path.name}:{number}")
            fleet.setdefault(measurement.engine_id, []).append(measurement)
    if not fleet:
        raise DatasetError(f"{path}: no measurements")

    ordered = {}
    for engine_id in sorted(fleet):
        history = sorted(fleet[engine_id], key=lambda m: m.cycle)
        cycles = [m.cycle for m in history]
        if cycles != list(range(1, len(history) + 1)):
            raise DatasetError(f"{path.name}: engine {engine_id} has missing or duplicate cycles")
        ordered[engine_id] = history
    return ordered


def summary(fleet: dict[int, list[Measurement]]) -> dict[str, int]:
    lengths = [len(history) for history in fleet.values()]
    return {"engines": len(fleet), "rows": sum(lengths), "min_cycles": min(lengths), "max_cycles": max(lengths)}
