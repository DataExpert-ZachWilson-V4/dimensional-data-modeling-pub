CREATE TABLE IF NOT EXISTS ChrisTaulbee.actors_history_scd (
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  latest_year INTEGER,
  start_date DATE,
  end_date DATE
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['quality_class']
  )
