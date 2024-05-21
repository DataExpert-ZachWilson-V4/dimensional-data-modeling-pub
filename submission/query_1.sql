-- Create table to store information about actors
CREATE TABLE actors (
    actor VARCHAR,  -- Name of the actor
    actor_id VARCHAR,  -- Unique identifier for the actor
    films ARRAY(ROW(
        year INTEGER, -- 'year': Release year of the film
        film VARCHAR, 
        votes INTEGER, 
        rating DOUBLE, 
        film_id VARCHAR -- Unique identifier for the film
    )),  -- Array of films with their details
    quality_class VARCHAR,  -- Categorical bucketing of the average rating of the movies
    is_active BOOLEAN,  -- Indicates whether an actor is currently active
    current_year INTEGER  -- Year this row represents for the actor
)
WITH (
    FORMAT = 'PARQUET',  -- Data format
    partitioning = ARRAY['current_year']  -- Partitioned by current_year
)
