/* DDL to create actors table with relevant fields */

CREATE TABLE supreethkabbin.actors (
  actor VARCHAR, 
  -- actor_id is a unique identifier and part of the primary key for actor_films
  actor_id VARCHAR, 
  -- array of struct with all attributes pertaining to actor
  films ARRAY(
    ROW(
      year INTEGER,
      film VARCHAR, 
      votes INTEGER, 
      rating DOUBLE, 
      -- film_id is a unique identifier and part of primary key for actor_films
      film_id VARCHAR
    )), 
  quality_class VARCHAR, 
  is_active BOOLEAN, 
  current_year INTEGER
)
WITH (
  format = 'PARQUET', 
  partitioning = ARRAY['current_year']
)