-- query_3
CREATE TABLE actors_history_scd (
  actor VARCHAR,
  quality_class VARCHAR,
  is_active INTEGER,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
