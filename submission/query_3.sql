CREATE TABLE actors_history_scd

(
    
  quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INT
  start_date INTEGER, 
  end_date INTEGER
)

WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
