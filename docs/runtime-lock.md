# Runtime lock

Versions fixed by `docs/guides/00_CONTRATS_COMMUNS.md` section 1. A tag counts as working only once
the "Verified" column names a real command run on a dated machine.

| Component | Pinned version / image | Verified |
|---|---|---|
| Spark / PySpark | `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu`, extended by `infra/Dockerfile.spark` into `smartpredict/spark:3.5.3` | 2026-11-21, Ayman's PC: image built; master + 2 workers ALIVE (1 core / 1024 MB each) |
| Hadoop / HDFS | `apache/hadoop:3.3.6` | 2026-11-21: NameNode + 2 DataNodes live, `fsck` average block replication 2.0 |
| Kafka (KRaft) | `apache/kafka:3.8.1` | 2026-11-21: healthy, topic `engine.measurements.v1` 3 partitions / RF 1 |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.3` + token-provider 3.5.3, kafka-clients 3.4.1, commons-pool2 2.11.1 (baked in) | 2026-11-21: `streaming/archive.py` consumed 20,837 events |
| PostgreSQL JDBC | 42.7.4 (baked in) | not used yet (writers use psycopg2) |
| PostgreSQL | `postgres:16` | 2026-11-21: healthy, `database/schema.sql` applied (3 tables) |
| Streamlit | 1.39.0 in `infra/Dockerfile.dashboard` | NOT YET |
| Docker / Compose on dev PC | Docker 29.3.1, Compose v5.1.1 | 2026-11-21 |

## Evidence collected on 2026-11-21 (Ayman's PC, 16 GB, low free RAM)

| Check | Command | Result |
|---|---|---|
| Compose syntax | `docker compose config --quiet` (all profiles) | OK |
| Kafka topic, idempotent | `scripts/bootstrap.sh --only kafka` twice | CREATED, then EXISTS, 3 partitions |
| Producer on real FD001 | `run_test_replay.sh --engine-ids 1,2,3` then console consumer | 206 events, 0 rejected; one partition per engine key; cycles strictly in order |
| HDFS replication | `hdfs fsck ... -locations` on a test file | `Live_repl=2`, HEALTHY |
| Distributed Spark | `infra/healthchecks/spark_distributed_check.py` | 200 tasks on spark-worker-1, 200 on spark-worker-2; HDFS read OK |
| Full bootstrap | `scripts/bootstrap.sh --strict` | topic, 7 HDFS dirs, 2 live DataNodes, schema.sql applied |
| Cold path B1 | `spark-archive` + `run_train_replay.sh` (contract_schema=yes) | Bronze: train 20,631 rows = 20,631 logical, 100 engines; test 206; 0 rejected; fsck replication 2.0 |
| Reda batch on Bronze | `batch/prepare.py`, `kpis.py`, `publish_kpis.py` | Silver 20,631 rows; Gold 93 KPI rows; 93 rows in `batch_kpis` |

## Issues found and fixed during the live run

1. Spark master healthcheck never passed: `/bin/sh` is `dash` in the Ubuntu Spark image (no `/dev/tcp`)
   and the master binds to `spark-master`, not `localhost`. Now `bash -c 'exec 3<>/dev/tcp/spark-master/7077'`.
2. Streaming progress logs skipped batches (only `lastProgress` was read every 15 s). Now iterates `recentProgress`.
3. Git Bash rewrites `/opt/...` arguments to `docker compose run` into Windows paths: prefix with
   `MSYS_NO_PATHCONV=1` or run from PowerShell.

## Resource notes

- With every service up (Kafka, NN, 2 DN, master, 2 workers, Postgres) containers used about 3.3 GiB;
  host free RAM went as low as 0.27 GiB during the first Spark job, without OOM or container restart.
- The cluster has 2 cores in total and each Spark app takes `spark.cores.max=2`, so streaming jobs and batch
  jobs run one after another (stop `spark-archive` with `docker compose stop`, checkpoint kept).

## Host port conflicts

- `5432` is used by a local `postgres` (pid 7200) on the dev PC: `.env` sets `POSTGRES_HOST_PORT=5433`.
