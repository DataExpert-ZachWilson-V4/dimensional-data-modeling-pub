CREATE TABLE IF NOT EXISTS actors_history_scd (
  actor VARCHAR,
  is_active BOOLEAN,
  quality_class VARCHAR,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH (
  format = 'parquet',
  partitioning = ARRAY['current_year']
)
