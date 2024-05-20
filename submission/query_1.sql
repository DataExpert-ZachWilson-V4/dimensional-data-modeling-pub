-- Query to create an actors table
-- Column quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year
CREATE OR REPLACE TABLE RaviT.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY( -- Column to store an array of films, each containing the following details:
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating INTEGER,
      film_id VARCHAR
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER 
)
WITH
  (
    FORMAT = 'PARQUET', -- Store the table in Parquet format
    partitioning = ARRAY['current_year'] -- Partition the table by the current_year column
  )
