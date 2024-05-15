CREATE OR REPLACE TABLE mposada.actors_history_scd ( -- create a type 2 slow changing dimension table that tracks changes to quality class and is_active
  actor VARCHAR, 
  actor_id VARCHAR, 
  quality_class VARCHAR, 
  is_active BOOLEAN, 
  start_date INTEGER, 
  end_date INTEGER,      -- start and end date indicate time when the record is active
  current_year INTEGER) 
  WITH
  (
    FORMAT = 'PARQUET',  -- Parquet for more efficient storage
    partitioning = ARRAY['current_year'] -- partitioned by current year to be able to query data at its state during different years
  )
