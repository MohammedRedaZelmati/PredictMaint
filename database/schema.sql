CREATE TABLE IF NOT EXISTS predictions (
  run_id UUID NOT NULL,
  event_id TEXT NOT NULL,
  model_version TEXT NOT NULL,
  source_split TEXT NOT NULL CHECK (source_split IN ('train', 'test')),
  engine_id INTEGER NOT NULL CHECK (engine_id > 0),
  cycle INTEGER NOT NULL CHECK (cycle > 0),
  event_ts TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ NOT NULL,
  predicted_at TIMESTAMPTZ NOT NULL,
  predicted_rul_cycles DOUBLE PRECISION NOT NULL,
  health_status TEXT NOT NULL CHECK (health_status IN ('HEALTHY', 'WARNING', 'CRITICAL')),
  PRIMARY KEY (run_id, event_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_prediction_latest
  ON predictions (run_id, engine_id, cycle DESC);
CREATE INDEX IF NOT EXISTS idx_prediction_time
  ON predictions (predicted_at DESC);

CREATE TABLE IF NOT EXISTS batch_kpis (
  snapshot_id TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  dimension_name TEXT NOT NULL DEFAULT 'all',
  dimension_value TEXT NOT NULL DEFAULT 'all',
  metric_value DOUBLE PRECISION NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (snapshot_id, metric_name, dimension_name, dimension_value)
);

CREATE TABLE IF NOT EXISTS model_registry (
  model_version TEXT PRIMARY KEY,
  model_hdfs_uri TEXT NOT NULL,
  feature_schema_hash TEXT NOT NULL,
  algorithm TEXT NOT NULL,
  validation_rmse DOUBLE PRECISION,
  test_rmse DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
