# Evidence from the live run on Ayman's PC (2026-11-21/22)

Files copied as they are from the running cluster (HDFS, PostgreSQL, container logs), not regenerated.
Commands and context are in `docs/runtime-lock.md`.

| File | Source | What it shows |
|---|---|---|
| `model_v001/comparison.csv` | `hdfs:///smartpredict/models/metadata/v001/comparison.csv` | All 11 candidates on the 60 grouped validation snapshots: B0 median (RMSE 62.23), LinearRegression (diverged, RMSE ~1.04e7), RandomForest (23.34) and the 8 GBT settings (22.37 to 24.45); fit time per candidate |
| `model_v001/manifest.json` | same folder | Frozen v001 manifest: GBTRegressor maxDepth 5 / maxIter 60 / stepSize 0.05, feature hash `05ad82b0...`, cap 125, split seed 42 |
| `model_v001/metrics.json` | same folder | Validation RMSE 22.37 / MAE 17.14 (60 snapshots); official test RMSE 17.23 / MAE 12.77 (100 terminal engines) |
| `model_v001/predictions_test.csv` | same folder | Per-engine official test: last observed cycle, official RUL, capped target, raw and clamped prediction |
| `kpi_snapshot_164.csv` | PostgreSQL `batch_kpis`, snapshot `d0e91350...` | The 164 KPI rows published from merged `main` on 2026-11-22 |
| `logs/archive_v1.log` | `docker logs spark-archive`, `[archive_v1]` lines | Received / rejected counts per micro-batch since the container was created (19:34Z on 2026-11-21), including the 4/4 rejected events on 2026-11-22. Before the logging fixes (runtime-lock issues 2 and 8), some batches were not printed, so these lines do **not** add up to the Bronze total: the authoritative counts come from `infra/healthchecks/bronze_count.py` (runtime-lock) |
| `logs/inference_v1.log` | `docker logs spark-inference`, `[inference_v1]` lines | Model load, every scored micro-batch, checkpoint resume and the 4/4 rejected contract-breaking events |
| `logs/contract_negative_events.txt` | Sent with `kafka-console-producer.sh` | The 4 synthetic contract-breaking events (key and JSON) used for the PR #7 live check |
| `logs/kpi_main_run.log` | `spark-client` output | KPI job and publication from merged `main`: 164 rows, snapshot id |
| `dashboard_full_test_run.jpg` | Streamlit capture on :8501 | Dashboard after the full test replay (100 engines) |

Values are SYNTHETIC only in `contract_negative_events.txt`; everything else comes from the NASA FD001
run. No `.env` value or password appears in these files.
