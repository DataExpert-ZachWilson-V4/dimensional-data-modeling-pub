CREATE OR REPLACE TABLE sagararora492.actors_history_scd
(
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_year INTEGER,
  end_year INTEGER
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY['start_year'] 
)