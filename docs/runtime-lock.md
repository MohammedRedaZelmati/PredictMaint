# Runtime lock (lot A1)

Versions fixed by `docs/guides/00_CONTRATS_COMMUNS.md` section 1. A tag is **not** claimed to work
until the "Verified" column says so with a real command and date.

| Component | Pinned version / image | Verified |
|---|---|---|
| Spark / PySpark | `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (extended by `infra/Dockerfile.spark`) | NOT YET (image not pulled) |
| Hadoop / HDFS | `apache/hadoop:3.3.6` | NOT YET |
| Kafka (KRaft) | `apache/kafka:3.8.1` | NOT YET |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.3` (+ token-provider 3.5.3, kafka-clients 3.4.1, commons-pool2 2.11.1), baked into the image | NOT YET |
| PostgreSQL JDBC | 42.7.4, baked into the image | NOT YET |
| PostgreSQL | `postgres:16` (minor/digest to lock after first pull) | NOT YET |
| Streamlit | 1.39.0 in `infra/Dockerfile.dashboard` (provisional, Reda confirms) | NOT YET |
| Docker / Compose on dev PC | Docker 29.3.1, Compose v5.1.1 | version printed 2026-11-21 |

## What has actually been run (2026-11-21, Ayman's PC)

- `docker compose config` -- see lot A1 report; validates syntax and interpolation only.
- Nothing else. **Docker Desktop / WSL were stopped and host RAM was 1.9 GiB available of 15.18 GiB**,
  below the 3.5 GiB gate from the common contract, so no image pull, no container start, no Spark job.

## Known unverified assumptions (to test first when RAM allows)

1. `apache/hadoop:3.3.6` reads `/opt/hadoop/etc/hadoop/{core,hdfs}-site.xml` from bind mounts and runs
   `hdfs namenode` / `hdfs datanode` as root with `HDFS_*_USER=root`.
2. NameNode format-on-first-run guard `[ -d /hadoop/dfs/name/current ]` matches the volume layout.
3. `apache/kafka:3.8.1` honours `KAFKA_HEAP_OPTS` and the listed `KAFKA_*` environment variables.
4. `apt-get install python3-pip` works in the Spark ubuntu image and `pip3` installs without
   `--break-system-packages` (Ubuntu 22.04 base expected; Ubuntu 24.04 would need the flag).
5. Spark executors fit in a 1 GiB worker (`spark.executor.memory=640m` + overhead).
6. Client-mode drivers in containers are reachable from workers through `spark.driver.host`.

## Host port conflicts found

- `5432` is already used by a local `postgres` process (pid 7200) on the dev PC. Use
  `POSTGRES_HOST_PORT=5433` in `.env`; container-internal address stays `postgres:5432`.
- `8080, 9870, 29092, 8501, 9092, 8020, 7077` were free.
