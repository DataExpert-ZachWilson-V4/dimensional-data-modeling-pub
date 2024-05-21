CREATE TABLE IF NOT EXISTS actors_history_scd (
  actor VARCHAR,
  is_active BOOLEAN,  -- SCD
  quality_class VARCHAR,  -- SCD
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH (
  format = 'parquet',
  partitioning = ARRAY['current_year']
)
