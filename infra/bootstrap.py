"""Idempotent initialisation of Kafka, HDFS and PostgreSQL for SmartPredict V1.

Runs inside the `spark-client` container (see scripts/bootstrap.sh). It only ever
*creates* what is missing: it never deletes a topic, a directory, a table or a row,
so a second run is a no-op apart from its checks.

Contracts: docs/guides/00_CONTRATS_COMMUNS.md sections 2, 3 and 5.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

TOPIC = "engine.measurements.v1"
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION = 1

HDFS_ROOT = "/smartpredict"
# Only parents are created; Spark creates job outputs, model versions and the
# exclusive checkpoint directories (archive_v1, inference_v1) itself.
HDFS_DIRS = (
    "bronze/engine_events",
    "silver/train_labeled",
    "gold/batch_kpis",
    "reference/test_rul",
    "models/rul_pipeline",
    "models/metadata",
    "checkpoints",
)

SCHEMA_SQL = Path("database/schema.sql")  # owned by Reda, applied as-is, never rewritten


def log(status: str, message: str) -> None:
    print(f"[bootstrap] {status:<7} {message}", flush=True)


def retry(action, what: str, attempts: int, delay: float):
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:  # noqa: BLE001 - report any connection failure
            if attempt == attempts:
                raise RuntimeError(f"{what} unavailable after {attempts} attempts: {error}") from error
            log("WAIT", f"{what} not ready ({error.__class__.__name__}), retry {attempt}/{attempts}")
            time.sleep(delay)


# ------------------------------------------------------------------ Kafka
def ensure_topic(bootstrap_servers: str, attempts: int, delay: float) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = retry(
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="smartpredict-bootstrap"),
        f"Kafka {bootstrap_servers}", attempts, delay,
    )
    try:
        if TOPIC not in admin.list_topics():
            try:
                admin.create_topics([NewTopic(TOPIC, TOPIC_PARTITIONS, TOPIC_REPLICATION)])
                log("CREATED", f"topic {TOPIC} ({TOPIC_PARTITIONS} partitions, replication {TOPIC_REPLICATION})")
            except TopicAlreadyExistsError:
                log("EXISTS", f"topic {TOPIC} (created concurrently)")
        else:
            log("EXISTS", f"topic {TOPIC}")
        partitions = admin.describe_topics([TOPIC])[0]["partitions"]
        if len(partitions) != TOPIC_PARTITIONS:
            # Never delete or repartition silently: the contract says 3, report the drift.
            raise RuntimeError(f"topic {TOPIC} has {len(partitions)} partitions, contract requires {TOPIC_PARTITIONS}")
        log("OK", f"topic {TOPIC} has {len(partitions)} partitions")
    finally:
        admin.close()


# ------------------------------------------------------------------ HDFS (WebHDFS, no JVM needed)
def webhdfs(namenode_http: str, path: str, op: str, method: str = "GET") -> dict:
    url = f"{namenode_http}/webhdfs/v1{path}?op={op}&user.name=root"
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read() or b"{}")


def ensure_hdfs(namenode_http: str, attempts: int, delay: float) -> None:
    retry(lambda: webhdfs(namenode_http, "/", "GETFILESTATUS"), f"WebHDFS {namenode_http}", attempts, delay)
    for relative in HDFS_DIRS:
        path = f"{HDFS_ROOT}/{relative}"
        try:
            status = webhdfs(namenode_http, path, "GETFILESTATUS")["FileStatus"]
            if status["type"] != "DIRECTORY":
                raise RuntimeError(f"{path} exists but is a {status['type']}, not a directory")
            log("EXISTS", f"hdfs {path}")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            # MKDIRS is idempotent and keeps existing content.
            if not webhdfs(namenode_http, path, "MKDIRS", "PUT").get("boolean"):
                raise RuntimeError(f"MKDIRS failed for {path}")
            log("CREATED", f"hdfs {path}")

    jmx = f"{namenode_http}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
    with urllib.request.urlopen(jmx, timeout=15) as report:
        state = json.loads(report.read())["beans"][0]
    live_datanodes = state.get("NumLiveDataNodes")
    log("OK" if live_datanodes == 2 else "WARN", f"hdfs live DataNodes = {live_datanodes} (contract: 2)")


# ------------------------------------------------------------------ PostgreSQL
def ensure_postgres(strict: bool, attempts: int, delay: float) -> None:
    import psycopg2

    params = dict(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "smartpredict"),
        user=os.environ.get("POSTGRES_USER", "smartpredict"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    connection = retry(lambda: psycopg2.connect(**params), f"PostgreSQL {params['host']}", attempts, delay)
    try:
        log("OK", f"database {params['dbname']} reachable")
        if not SCHEMA_SQL.is_file():
            message = f"{SCHEMA_SQL} not found: waiting for Reda's contracts PR (no parallel DDL is written here)"
            if strict:
                raise RuntimeError(message)
            log("BLOCKED", message)
            return
        # Reda's DDL uses CREATE ... IF NOT EXISTS, so re-applying it is idempotent.
        with connection, connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        log("OK", f"applied {SCHEMA_SQL}")
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--namenode-http", default=os.environ.get("NAMENODE_HTTP", "http://namenode:9870"))
    parser.add_argument("--only", choices=["kafka", "hdfs", "postgres"], action="append",
                        help="limit to some services (repeatable); default: all")
    parser.add_argument("--strict", action="store_true", help="fail if Reda's database/schema.sql is missing")
    parser.add_argument("--attempts", type=int, default=20)
    parser.add_argument("--delay", type=float, default=3.0)
    parser.add_argument("--dry-run", action="store_true", help="print the plan and contact nothing")
    args = parser.parse_args(argv)
    targets = args.only or ["kafka", "hdfs", "postgres"]

    if args.dry_run:
        log("PLAN", f"kafka {args.kafka}: topic {TOPIC} x{TOPIC_PARTITIONS}, replication {TOPIC_REPLICATION}")
        for relative in HDFS_DIRS:
            log("PLAN", f"hdfs mkdir -p {HDFS_ROOT}/{relative}")
        log("PLAN", f"postgres: apply {SCHEMA_SQL} if present ({'present' if SCHEMA_SQL.is_file() else 'missing'})")
        return 0

    failures = 0
    steps = {
        "kafka": lambda: ensure_topic(args.kafka, args.attempts, args.delay),
        "hdfs": lambda: ensure_hdfs(args.namenode_http, args.attempts, args.delay),
        "postgres": lambda: ensure_postgres(args.strict, args.attempts, args.delay),
    }
    for name in targets:
        try:
            steps[name]()
        except Exception as error:  # noqa: BLE001 - keep going, report every failing service
            failures += 1
            log("FAILED", f"{name}: {error}")
    log("DONE", "all requested services initialised" if not failures else f"{failures} service(s) failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
