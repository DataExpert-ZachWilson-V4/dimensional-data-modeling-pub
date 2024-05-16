CREATE OR REPLACE TABLE lsleena.actors (
    actor VARCHAR,          -- Stores the actor's name
    actor_id VARCHAR,       -- Unique identifier for each actor
    films ARRAY(
        ROW(
            year INTEGER,   -- Release year of the film
            film VARCHAR,   -- Name of the film
            votes INTEGER,  -- Number of votes the film received
            rating DOUBLE,  -- Rating of the film
            film_id VARCHAR -- Unique identifier for each film
        )),
    quality_class VARCHAR,  -- Categorical rating based on average rating in the most recent year
    is_active BOOLEAN,      -- Indicates if the actor is currently active
    current_year INTEGER    -- Represents the year this row is relevant for the actor
)
WITH
(
FORMAT = 'PARQUET',
partitioning = ARRAY['current_year']
)