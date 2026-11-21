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
