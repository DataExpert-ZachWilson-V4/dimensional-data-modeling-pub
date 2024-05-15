-- Creating a new table named 'actors' with 'films' as an array of struct 
-- and 'quality_class' for storing categorical bucketing of the average rating 
-- of the movies for this actor in their most recent year
CREATE 
OR REPLACE TABLE mariavyso.actors (
    actor VARCHAR NOT NULL,
    actor_id VARCHAR NOT NULL,
    -- Array for multiple films associated with each actor. Each row contains film details.
    films ARRAY(
        ROW(
            year INTEGER,
            film VARCHAR,
            votes INTEGER,
            rating DOUBLE,
            film_id VARCHAR
        )
    ),
    -- Categorical rating based on average rating in the most recent year
    quality_class VARCHAR,
    -- Indicates if the actor is currently active
    is_active BOOLEAN,
    -- Represents the year this row is relevant for the actor
    current_year INTEGER
)
WITH
    (
        FORMAT = 'PARQUET',
        partitioning = ARRAY['current_year']
    )
