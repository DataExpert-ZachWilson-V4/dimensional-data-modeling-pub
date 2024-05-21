--Create table statement to generate actors table 
CREATE OR REPLACE TABLE amaliah21315.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY (
    ROW ( -- Array to store the details of each film per actor
      film VARCHAR, -- the name of the film
      YEAR INTEGER, -- the year of the film
      votes INTEGER, -- the votes the film received that year
      rating DOUBLE, -- the rating the film received that year
      film_id VARCHAR -- defined as varchar to store the format of field thats in film_id actor_films
    )
  ),
  average_rating DOUBLE,
  quality_class VARCHAR, --column to store the average rating category
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET', -- the format that we would like to store the data
    partitioning = ARRAY['current_year'] -- partitions the table by the current film year
  )
