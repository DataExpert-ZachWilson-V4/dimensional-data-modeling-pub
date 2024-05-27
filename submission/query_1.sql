CREATE OR REPLACE TABLE jrsarrat.actors ( --create an actors table with applicable data types
  actor VARCHAR, 
  actor_id VARCHAR, 
  films ARRAY(ROW( --this struct provides a table of data consisting of film attributes
    film VARCHAR,
    year INTEGER,
    votes INTEGER,
    rating DOUBLE,
    film_id VARCHAR
  )), 
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER 
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
