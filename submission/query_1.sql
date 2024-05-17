-- query_1

-- Create a table named 'actors' to store information about actors
CREATE TABLE actors (
  actor VARCHAR,
  actor_id VARCHAR,
  -- Array to store details of films the actor has been in
  films ARRAY(
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    -- Partitioning the table based on 'current_year' to optimize query performance
	FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
  
