# Runbook (infra, in progress)

Only commands that exist today. Later lots (bootstrap, producer, archive, inference) extend this file.

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

Put the official NASA C-MAPSS files in `data/` (git-ignored): `train_FD001.txt`, `test_FD001.txt`
(and `RUL_FD001.txt`, which the producer never reads). Inside containers the repo is mounted at
`/opt/smartpredict`, so `data/` is visible there too.

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

Spark tests: `docker compose run --rm spark-client python3 -m pytest tests/test_streaming_sink.py`
(needs pytest in the image or `pip install --user pytest` in the container).
