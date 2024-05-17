-- query_3

-- Create a table named 'actors_history_scd' to store historical data of actors with SCD
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
    -- Partitioning the table based on 'current_year' to optimize query performance
	FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
  
