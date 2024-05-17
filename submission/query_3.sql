-- Note this table contains 2 SCDs: quality_class & is_active
-- note start/end/current date is actually year, following the naming conventions of hw readme

CREATE TABLE derekleung.actors_history_scd (
  actor_id VARCHAR(9),
  actor VARCHAR(63),
  quality_class VARCHAR(7),
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
