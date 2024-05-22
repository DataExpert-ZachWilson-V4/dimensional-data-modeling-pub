CREATE OR REPLACE TABLE ttian45759.actors_history_scd (
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE,
  current_year INTEGER
)

WITH (
    FORMAT='parquet',
    partitioning=ARRAY['current_year']
)