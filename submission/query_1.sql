-- Cummulative dimension table to store actor information
CREATE OR REPLACE TABLE srik1981.actors (
    -- Stores the actor's name
    actor VARCHAR,
    -- Unique Identifier associated with each actor
    actor_id VARCHAR,
    -- An Array of ROWS for multiple films associated with each actor
    films ARRAY(
        ROW(
            -- Release year of the film
            year INTEGER,
            -- Name of the film
            film VARCHAR,
            -- Number of votes the film received
            votes INTEGER,
            -- Rating of the film
            rating DOUBLE,
            -- Unique identifier for each film
            film_id VARCHAR
        )
    ),
    -- Categorical rating based on average rating in the most recent year
    quality_class VARCHAR,
    -- Indicates if the actor is active currently, based on the movie made in the current year
    is_active BOOLEAN,
    -- The year this row represents for this actor
    current_year INTEGER
) WITH (
    -- Storage format
    format = 'PARQUET',
    -- Partition data by current_year which helps in optimizing query
    partitioning = ARRAY ['current_year']
)