create or replace table actors_history_scd(
actor varchar,
quality_class varchar,
is_active boolean,
start_date integer,
end_date integer,
current_year integer
)
with
  (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
