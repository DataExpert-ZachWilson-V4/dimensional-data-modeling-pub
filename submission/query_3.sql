CREATE OR REPLACE TABLE ivomuk37854.actors_history_scd (
--actor_id VARCHAR,
actor VARCHAR,
quality_class VARCHAR,
is_active BOOLEAN,
start_date INTEGER,
end_date INTEGER,
current_year INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
)
