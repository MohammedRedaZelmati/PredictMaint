"""Read FD001 turbine measurements and derive causal features."""

from pathlib import Path

import numpy as np


def read_fleet(path: Path) -> dict[int, np.ndarray]:
    rows = np.loadtxt(path, ndmin=2)
    if rows.shape[0] == 0 or rows.shape[1] != 26 or not np.isfinite(rows).all():
        raise ValueError(f"{path}: expected nonempty, finite rows with 26 columns")

    ids = rows[:, 0]
    if (ids < 1).any() or (ids != np.floor(ids)).any():
        raise ValueError(f"{path}: engine IDs must be positive integers")

    fleet = {}
    for engine in np.unique(ids):
        history = rows[ids == engine, 1:]
        history = history[history[:, 0].argsort()]
        if not np.array_equal(history[:, 0], np.arange(1, len(history) + 1)):
            raise ValueError(f"{path}: engine {engine:g} has missing or duplicate cycles")
        fleet[int(engine)] = history
    return fleet


def read_test_targets(path: Path, fleet: dict[int, np.ndarray]) -> np.ndarray:
    values = np.loadtxt(path, ndmin=2)
    if values.shape != (len(fleet), 1) or list(fleet) != list(range(1, len(fleet) + 1)):
        raise ValueError(f"{path}: expected one value per test engine in ID order")
    targets = values[:, 0]
    if not np.isfinite(targets).all() or (targets < 0).any() or (targets != np.floor(targets)).any():
        raise ValueError(f"{path}: expected nonnegative integer RUL values")
    return targets


def training_targets(length: int) -> np.ndarray:
    """Run-to-failure trajectories end at zero remaining cycles."""
    return np.minimum(np.arange(length - 1, -1, -1), 125)


def active_sensors(fleet: dict[int, np.ndarray]) -> np.ndarray:
    sensors = np.concatenate([history[:, 4:] for history in fleet.values()])
    return np.flatnonzero(sensors.std(axis=0) > 1e-8)


def describe(fleet: dict[int, np.ndarray], selected: np.ndarray) -> None:
    lengths = np.array([len(history) for history in fleet.values()])
    print(f"Training rows: {lengths.sum()} | engines: {len(fleet)}")
    print(f"Cycles to failure (min / median / max): {lengths.min()} / {np.median(lengths):g} / {lengths.max()}")
    names = ", ".join(str(sensor + 1) for sensor in selected)
    print(f"Nonconstant training sensors: {names or 'none'}")


def make_features(history: np.ndarray, selected: np.ndarray) -> np.ndarray:
    """Combine current readings, a trailing mean and a short-term sensor change."""
    readings = history[:, 4:][:, selected]
    end = np.arange(1, len(history) + 1)
    start = np.maximum(end - 15, 0)
    prefix = np.concatenate((np.zeros((1, len(selected))), readings.cumsum(axis=0)))
    average = (prefix[end] - prefix[start]) / (end - start)[:, None]
    change = readings - readings[np.maximum(end - 6, 0)]
    return np.column_stack((history[:, :4], readings, average, change))
