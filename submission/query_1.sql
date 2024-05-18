-- create the actors table in the jlcharbneau schema
--  including the actor_id, actor (name), an array/struct of the films the actor has been in,
--  a quality_class field (for store analysis), is active, and the current year
-- Create the new table
CREATE OR REPLACE TABLE jlcharbneau.actors (
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