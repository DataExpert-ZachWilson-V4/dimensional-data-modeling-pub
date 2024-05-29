-- Create a table to store historical data for actors using Slowly Changing Dimension (SCD) technique
CREATE OR REPLACE TABLE luiscoelho37431.actors_history_scd (
  -- Define columns names and types
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET', -- Storage format for the table
    partitioning = ARRAY['current_year'] -- Partition the data based on the current_year column
  ) 