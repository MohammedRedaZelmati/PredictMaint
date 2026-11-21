"""Read-only PostgreSQL access for the SmartPredict dashboard."""

import os
from contextlib import closing

import psycopg2
from psycopg2.extras import RealDictCursor


def fetch_rows(query, parameters=()):
    with closing(psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "smartpredict"),
        user=os.environ.get("POSTGRES_USER", "smartpredict"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, parameters)
            return [dict(row) for row in cursor.fetchall()]


def list_test_runs():
    return fetch_rows("""
        SELECT run_id, MAX(predicted_at) AS last_prediction_at
        FROM predictions WHERE source_split = 'test'
        GROUP BY run_id ORDER BY last_prediction_at DESC
    """)


def latest_by_engine(run_id):
    return fetch_rows("""
        SELECT DISTINCT ON (engine_id)
          engine_id, cycle, predicted_rul_cycles, health_status,
          model_version, predicted_at, published_at,
          EXTRACT(EPOCH FROM predicted_at - published_at) AS latency_seconds
        FROM predictions
        WHERE source_split = 'test' AND run_id = %s
        ORDER BY engine_id, cycle DESC, predicted_at DESC
    """, (run_id,))


def engine_trajectory(run_id, engine_id):
    return fetch_rows("""
        SELECT cycle, predicted_rul_cycles, health_status, predicted_at
        FROM predictions
        WHERE source_split = 'test' AND run_id = %s AND engine_id = %s
        ORDER BY cycle
    """, (run_id, engine_id))


def latest_kpis():
    return fetch_rows("""
        SELECT snapshot_id, metric_name, dimension_name, dimension_value,
               metric_value, computed_at
        FROM batch_kpis
        WHERE snapshot_id = (
          SELECT snapshot_id FROM batch_kpis ORDER BY computed_at DESC LIMIT 1
        )
        ORDER BY metric_name, dimension_name, dimension_value
    """)


def model_registry(model_version):
    return fetch_rows("""
        SELECT model_version, algorithm, validation_rmse, test_rmse, created_at
        FROM model_registry WHERE model_version = %s
    """, (model_version,))
