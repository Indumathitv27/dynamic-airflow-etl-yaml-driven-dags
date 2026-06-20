CREATE TABLE IF NOT EXISTS etl_watermarks (
  pipeline_name TEXT PRIMARY KEY,
  last_success_ts TIMESTAMP,
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_weather (
  pipeline_name TEXT,
  fetched_at TIMESTAMP,
  payload JSONB
);

CREATE TABLE IF NOT EXISTS stg_weather_hourly (
  pipeline_name TEXT,
  ts TIMESTAMP,
  temperature DOUBLE PRECISION,
  windspeed DOUBLE PRECISION,
  fetched_at TIMESTAMP,
  CONSTRAINT uq_stg_weather UNIQUE (pipeline_name, ts, fetched_at)
);

CREATE TABLE IF NOT EXISTS pipeline_run_metrics (
  pipeline_name TEXT,
  dag_run_id TEXT,
  records_raw INT,
  records_loaded INT,
  dq_passed BOOLEAN,
  duration_seconds INT,
  run_ts TIMESTAMP DEFAULT NOW()
);
