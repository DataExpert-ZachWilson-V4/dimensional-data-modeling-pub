-- Create table to store information about actors
CREATE TABLE alissabdeltoro.actors (
    actor VARCHAR,  -- Name of the actor
    actor_id VARCHAR,  -- Unique identifier for the actor
    films ARRAY(ROW(
        film VARCHAR, 
        votes INTEGER, 
        rating DOUBLE, 
        film_id VARCHAR
    )),  -- Array of films with their details
    quality_class VARCHAR CHECK (quality_class IN ('star', 'good', 'average', 'bad')),  -- Categorical bucketing of the average rating of the movies
    is_active BOOLEAN,  -- Indicates whether an actor is currently active
    current_year INTEGER  -- Year this row represents for the actor
)
WITH (
    FORMAT = 'PARQUET',  -- Data format
    partitioned_by = ARRAY['current_year']  -- Partitioned by current_year
);
