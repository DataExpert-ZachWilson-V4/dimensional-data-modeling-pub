-- Drop the table if it exists
DROP TABLE IF EXISTS lmuraleedharanair68215.actors

-- Create the table
CREATE TABLE lmuraleedharanair68215.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(ROW(
    film VARCHAR,
    votes INTEGER, 
    rating DOUBLE,
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
