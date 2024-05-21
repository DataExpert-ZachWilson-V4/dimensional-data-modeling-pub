CREATE TABLE
  ningde95.actors_history_scd (
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INT,
    end_date INT,
    current_year INT
  )
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
