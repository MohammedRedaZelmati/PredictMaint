"""Idempotent PostgreSQL writer for hot-path predictions (lot A5).

Writes exactly the `predictions` table of Reda's database/schema.sql (contract section 5)
with an UPSERT on (run_id, event_id, model_version). Runs on Spark executors inside
`foreachPartition`, so nothing is collected on the driver. Any database error is re-raised
after bounded retries so the micro-batch fails and Spark retries it from the checkpoint.

Pure Python (psycopg2 imported lazily) so the logic is testable without Spark or a database.
"""

from __future__ import annotations

import math
import os
import time
from typing import Callable, Iterable

RUL_CAP = 125.0
CRITICAL_MAX = 20.0
WARNING_MAX = 50.0

PREDICTION_COLUMNS = (
    "run_id", "event_id", "model_version", "source_split", "engine_id", "cycle",
    "event_ts", "published_at", "predicted_at", "predicted_rul_cycles", "health_status",
)

UPSERT_SQL = (
    "INSERT INTO predictions (" + ", ".join(PREDICTION_COLUMNS) + ") VALUES %s "
    "ON CONFLICT (run_id, event_id, model_version) DO UPDATE SET "
    "source_split = EXCLUDED.source_split, engine_id = EXCLUDED.engine_id, cycle = EXCLUDED.cycle, "
    "event_ts = EXCLUDED.event_ts, published_at = EXCLUDED.published_at, "
    "predicted_at = EXCLUDED.predicted_at, predicted_rul_cycles = EXCLUDED.predicted_rul_cycles, "
    "health_status = EXCLUDED.health_status"
)
# run_id is UUID in SQL; everything else maps directly from Python types.
ROW_TEMPLATE = "(%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


def clamp_rul(prediction: float) -> float:
    """Contract: predicted_rul_cycles = max(0, min(125, prediction))."""
    # None, NaN or +/-inf: never invent a value by clamping it to 0 or 125.
    if prediction is None or not math.isfinite(prediction):
        raise ValueError(f"model returned no usable prediction: {prediction!r}")
    return max(0.0, min(RUL_CAP, float(prediction)))


def health_status(rul: float) -> str:
    """CRITICAL <= 20 < WARNING <= 50 < HEALTHY, on the clamped RUL."""
    if rul <= CRITICAL_MAX:
        return "CRITICAL"
    if rul <= WARNING_MAX:
        return "WARNING"
    return "HEALTHY"


def connection_params_from_env(environ=os.environ) -> dict:
    """Read on the driver; the dict is shipped to executors inside the task closure."""
    return {
        "host": environ.get("POSTGRES_HOST", "postgres"),
        "port": int(environ.get("POSTGRES_PORT", "5432")),
        "dbname": environ.get("POSTGRES_DB", "smartpredict"),
        "user": environ.get("POSTGRES_USER", "smartpredict"),
        "password": environ.get("POSTGRES_PASSWORD", ""),
        "connect_timeout": 10,
        "application_name": "smartpredict-inference",
    }


def row_params(row) -> tuple:
    """Spark Row (or mapping) with the contract columns -> SQL parameters in column order."""
    params = tuple(row[name] for name in PREDICTION_COLUMNS)
    missing = [name for name, value in zip(PREDICTION_COLUMNS, params) if value is None]
    if missing:
        raise ValueError(f"prediction row {row['event_id']!r} has no value for {missing}")
    return params


def _default_connect(params: dict):
    import psycopg2

    return psycopg2.connect(**params)


def _default_execute(cursor, rows: list[tuple], page_size: int) -> None:
    from psycopg2.extras import execute_values

    execute_values(cursor, UPSERT_SQL, rows, template=ROW_TEMPLATE, page_size=page_size)


def write_rows(
    rows: Iterable,
    params: dict,
    attempts: int = 3,
    backoff_s: float = 2.0,
    page_size: int = 500,
    connect: Callable = _default_connect,
    execute: Callable = _default_execute,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """UPSERT all rows of one partition in a single transaction; return the row count.

    Retries the whole transaction (idempotent thanks to the UPSERT key); after the last
    attempt the error propagates so Spark fails the micro-batch instead of losing data.
    """
    batch = [row_params(row) for row in rows]
    if not batch:
        return 0
    for attempt in range(1, attempts + 1):
        connection = None
        try:
            connection = connect(params)
            with connection:  # commit on success, rollback on exception
                with connection.cursor() as cursor:
                    execute(cursor, batch, page_size)
            return len(batch)
        except Exception:
            if attempt == attempts:
                raise
            sleep(backoff_s * attempt)
        finally:
            if connection is not None:
                connection.close()
    return 0  # unreachable
