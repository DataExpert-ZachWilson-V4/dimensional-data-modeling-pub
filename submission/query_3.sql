-- a DDL statement to create an `actors_history_scd` table that 
-- tracks the following fields for each actor in the `actors` table:
--    `quality_class`, `is_active`, `start_date`, `end_date`

-- table creation schema
CREATE OR REPLACE TABLE siawayforward.actors_history_scd (
    actor_id VARCHAR,
    -- adding actor's name for ease of users
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    -- we don't have dates, we have years for time dimension, that's the lowest time grain we can use
    start_date INTEGER,
    end_date INTEGER,
    -- we could default to using the first and last days of the year, but we don't need that level so year it is
    current_year INTEGER
)
-- we want our table to be partitioned and store column optimal read data
WITH (
  format='PARQUET',
  partitioning= ARRAY['current_year']
)