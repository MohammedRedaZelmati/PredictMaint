# Runbook (infra, ingestion, streaming)

Commands for Ayman's lots A1-A5, as run on the cluster on 2026-11-21/22. The full launch order with
Reda's batch, ML and dashboard steps is in the top-level `README.md`.

## Before any heavy command: RAM check (PowerShell)

```powershell
Get-CimInstance Win32_OperatingSystem | Select-Object @{N='AvailableGiB';E={[math]::Round($_.FreePhysicalMemory/1MB,2)}},@{N='TotalGiB';E={[math]::Round($_.TotalVisibleMemorySize/1MB,2)}}
```

Policy (common contract section 8): below 3.5 GiB available, start nothing heavy; 3.5-5 light tests;
5-7 one limited smoke test; 7+ integration may be tried under watch. Never `docker compose down -v`,
never delete volumes or checkpoints.

## Validate the compose file (no containers started)

```powershell
cp .env.example .env     # first time only; set POSTGRES_HOST_PORT=5433 if 5432 is taken
docker compose config --quiet
docker compose --profile jobs --profile tools --profile dashboard config --quiet
```

## Start the cluster in phases (RAM permitting)

```powershell
docker compose up -d kafka
docker compose up -d namenode datanode-1 datanode-2
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose up -d postgres
bash infra/healthchecks/check_cluster.sh
```

UIs: Spark `http://localhost:8080`, HDFS `http://localhost:9870`.

## Bootstrap (lot A2, idempotent)

```powershell
docker compose run --rm spark-client bash scripts/bootstrap.sh            # topic + HDFS dirs + SQL schema if present
docker compose run --rm spark-client bash scripts/bootstrap.sh --strict   # fail if Reda's database/schema.sql is missing
python infra/bootstrap.py --dry-run                                       # print plan, contact nothing
```

Creates only what is missing: topic `engine.measurements.v1` (3 partitions, replication 1),
`/smartpredict/{bronze/engine_events,silver/train_labeled,gold/batch_kpis,reference/test_rul,models/rul_pipeline,models/metadata,checkpoints}`,
and applies `database/schema.sql` (Reda's, `CREATE ... IF NOT EXISTS`). A second run only re-checks.
It never deletes a topic, a directory, a checkpoint or a row. It warns if HDFS has fewer than 2 live DataNodes.
Kafka uses kafka-python-ng's admin client, HDFS uses WebHDFS on `namenode:9870`, so no extra CLI is needed in the image.

## Dataset files (not in Git)

Put the official NASA C-MAPSS files in `data/raw/` (git-ignored): `train_FD001.txt`, `test_FD001.txt`
and `RUL_FD001.txt` (used only by `scripts/run_evaluate.sh`; the producer never reads it). Hashes are in
`docs/data-card.md`. Inside containers the repo is mounted at `/opt/smartpredict`, so `data/raw/` is
visible there too.

## Producer (lot A3)

```powershell
python -m ingestion.producer --split test --speed 0 --engine-ids 1 --limit 3 --dry-run   # print, no Kafka
docker compose run --rm spark-client bash scripts/run_train_replay.sh --engine-ids 1,2,3  # smoke first
docker compose run --rm spark-client bash scripts/run_train_replay.sh                     # full train replay
docker compose run --rm spark-client bash scripts/run_test_replay.sh --run-id <uuid>      # deliberate resume
```

- `--speed` = simulated cycles per second (each step sends one event per engine still running); 0 = no pacing.
- Order: cycle-major across engines, so each engine's events stay in cycle order; Kafka key
  `fd001:<split>:<engine_id>` pins an engine to one partition; `max_in_flight=1` keeps order on retry.
- `event_ts` = 2026-01-01T00:00:00Z + cycle x 60 s (simulated, override with `--event-origin`);
  `published_at` = real send time. No RUL or label field is ever sent.
- Invalid events are rejected before sending; exit code 1 if anything was rejected or not acknowledged.
- When Reda's `contracts/event.schema.json` exists, every event is also validated against it.

## Cold path: Kafka -> Bronze Parquet (lot A4)

```powershell
docker compose --profile jobs up -d spark-archive      # start BEFORE the train replay
docker compose logs -f spark-archive                   # per-batch received / rejected counts
```

Output `hdfs://namenode:8020/smartpredict/bronze/engine_events/source_split=<train|test>/`,
checkpoint `.../checkpoints/archive_v1/` (exclusive: never run two archive instances, never delete it).
Columns: every contract field (`event_ts`, `published_at` as timestamps) + `kafka_key`, `kafka_topic`,
`kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at`. Bronze is append-only:
a replay appends the same `event_id`s again; Silver deduplicates on `(source_split, engine_id, cycle)`.

Logical count check for Reda (Spark SQL, from `spark-client`):

```sql
SELECT source_split, COUNT(*) AS rows, COUNT(DISTINCT event_id) AS logical_rows
FROM parquet.`hdfs://namenode:8020/smartpredict/bronze/engine_events/` GROUP BY source_split;
```

Each message must carry exactly the 33 contract fields: an extra key (even with a `null` value, such as
`target_rul: null`) is rejected, and `event_ts` / `published_at` must be UTC strings (`Z` or `+00:00`),
checked on the original text before conversion. Rejected messages are counted in the job log
(`received` / `rejected`) and never reach Bronze.

Tests, including the Spark ones (pytest, pyspark and Java 17 are in the image; locally on Windows
the Spark tests are skipped because Java 25 is not supported by Spark 3.5):

```powershell
docker compose run --rm --no-deps spark-client python3 -m pytest -q tests
```

## Hot path: Kafka -> Spark ML -> PostgreSQL (lot A5)

Prerequisites (Reda): `contracts/feature_columns.json`, `database/schema.sql` applied by bootstrap,
`hdfs://namenode:8020/smartpredict/models/rul_pipeline/v001/` and `.../models/metadata/v001/manifest.json`.

```powershell
docker compose --profile jobs up -d spark-inference          # FIRST: fresh checkpoint reads from `latest`
docker compose logs -f spark-inference                       # wait for "model=... features=25"
docker compose run --rm spark-client bash scripts/run_test_replay.sh --engine-ids 1,2,3
```

Start-up refuses to run (exit 3, explicit message) if the manifest's `schema_version`, `model_version`,
`model_hdfs_uri`, `target_cap_cycles` or `feature_schema_hash` (SHA-256 of the compact JSON list of the
25 features) does not match. No fallback model, no heuristic.

Per micro-batch: validate events; refuse the batch if one `(run_id, event_id)` carries two different
measurement sets (SHA-256 fingerprint of the 24 measurements), otherwise drop identical duplicates;
`model.transform()` on the executors; clamp to [0, 125]; status CRITICAL <= 20 < WARNING <= 50 < HEALTHY;
`predicted_at` = scoring time; UPSERT from `foreachPartition` (psycopg2, one transaction per partition,
3 attempts). A NULL, NaN or infinite prediction, a conflicting duplicate or a database error fails the
micro-batch, and Spark retries it from checkpoint `inference_v1`.

Limit: the conflict check sees one micro-batch. A conflicting replay arriving in a later batch overwrites
the SQL row (the table stores no measurements); Bronze keeps both versions and Reda's
`deduplicate_bronze` gate refuses them in batch. A conflicting pair inside one batch blocks the stream
on every retry until it is resolved by hand: do not inject one on a shared checkpoint.

## Running streaming and batch jobs together

4 cores in total (2 workers x 2 cores / 1 GiB); each application takes `spark.cores.max=2` as
2 executors x 1 core x 450m, one on each worker. So at most two applications run at once: `spark-archive`
+ `spark-inference` during a replay, or one streaming job + one batch job. Stop one stream first
(`docker compose stop spark-archive`, checkpoint kept) before `scripts/run_batch.sh` or training.

## Evidence exported from the live run

`docs/evidence/` holds the model v001 metadata copied from HDFS (`comparison.csv`, `manifest.json`,
`metrics.json`, `predictions_test.csv`), the 164-row KPI snapshot, job log excerpts and a dashboard capture.
See `docs/evidence/README.md`.
