-- Create table to store actor information
CREATE OR REPLACE TABLE rajkgupta091041107.actors (
    -- Actor name
    actor VARCHAR,
    -- Actor's ID
    actor_id VARCHAR,
    -- Array of film details including film name, votes, rating, and film ID
    films ARRAY(ROW(
        film VARCHAR,
        votes BIGINT,
        rating DOUBLE,
        film_id VARCHAR
    )),
    -- Categorical bucketing of the average rating for the actor in their most recent year
    quality_class VARCHAR,
    -- Indicator for whether the actor is currently active
    is_active BOOLEAN,
    -- Year for which the row represents actor information
    current_year INT
)
WITH
(
    -- Specify the data format
    FORMAT = 'PARQUET',
    -- Partitioning by current_year
    partitioning = ARRAY['current_year']
)
