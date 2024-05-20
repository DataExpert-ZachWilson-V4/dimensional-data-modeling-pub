CREATE
OR REPLACE TABLE  halloweex.actors (
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
    );
