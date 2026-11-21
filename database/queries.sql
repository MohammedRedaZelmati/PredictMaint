-- Latest test replay for the dashboard.
SELECT run_id, MAX(predicted_at) AS last_prediction_at
FROM predictions
WHERE source_split = 'test'
GROUP BY run_id
ORDER BY last_prediction_at DESC
LIMIT 1;

-- Latest prediction per engine within a selected run.
SELECT DISTINCT ON (engine_id)
  engine_id, cycle, predicted_rul_cycles, health_status,
  model_version, predicted_at, published_at,
  EXTRACT(EPOCH FROM predicted_at - published_at) AS latency_seconds
FROM predictions
WHERE source_split = 'test' AND run_id = $1
ORDER BY engine_id, cycle DESC, predicted_at DESC;

-- RUL trajectory for one engine and one replay.
SELECT cycle, predicted_rul_cycles, health_status, predicted_at
FROM predictions
WHERE source_split = 'test' AND run_id = $1 AND engine_id = $2
ORDER BY cycle;

-- KPI values from the latest computed snapshot.
SELECT snapshot_id, metric_name, dimension_name, dimension_value, metric_value, computed_at
FROM batch_kpis
WHERE snapshot_id = (SELECT snapshot_id FROM batch_kpis ORDER BY computed_at DESC LIMIT 1)
ORDER BY metric_name, dimension_name, dimension_value;
