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
