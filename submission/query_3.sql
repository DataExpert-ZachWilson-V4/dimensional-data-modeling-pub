--Create table statement to generate actors slowly changing dimension table 
CREATE OR REPLACE TABLE amaliah21315.actors_history_scd 
(
    actor VARCHAR,
    actor_id VARCHAR,
    is_active BOOLEAN, 
    average_rating DOUBLE, -- column to store the average rating
    quality_class VARCHAR, --column to store the average rating category
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET', -- the format that we would like to store the data
    partitioning = ARRAY['current_year'] -- partitions the table by the current film year
  )
