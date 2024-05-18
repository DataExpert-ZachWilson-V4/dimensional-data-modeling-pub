CREATE OR REPLACE TABLE chinmay_hebbal.actors_history_scd (
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
