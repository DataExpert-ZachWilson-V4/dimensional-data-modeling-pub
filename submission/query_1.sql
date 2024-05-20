CREATE OR REPLACE TABLE positivelyamber.actors(
    -- Actor's name
    actor VARCHAR,
    -- Actor's ID
    actor_id VARCHAR,
    -- Array of films
    films ARRAY(
        ROW(
            -- Film name
            film VARCHAR,
            -- Number of votes the film received 
            votes INTEGER,
            -- Rating of the film
            rating DOUBLE,
            -- Unique id of the film
            film_id VARCHAR
        )),
    -- The bucket of the average rating of the movies for this actor in their most recent year
    quality_class VARCHAR,
    -- Whether the actor is making films this year
    is_active BOOLEAN,
    -- The year this row represents for the actor
    current_year INTEGER
)
WITH (
    -- Parquet formatting
    FORMAT = 'PARQUET',
    -- Partition by year
    partitioning = ARRAY['current_year']
)