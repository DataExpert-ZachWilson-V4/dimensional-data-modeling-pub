-- An SCD table table that tracks the following fields for each actor in the actors table:
-- * quality_class
-- * is_active
-- * start_date
-- * end_date
CREATE
OR REPLACE TABLE actors_history_scd (
    actor VARCHAR,
    -- actor: The name of the actor.
    actor_id VARCHAR,
    -- actor_id: A unique identifier for each actor.
    quality_class VARCHAR,
    -- quality_class: The average rating of the movies for this actor in their most recent year
    is_active BOOLEAN,
    -- is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year)
    start_year INTEGER,
    -- start_year: start year of the streak 
    -- (streak here is defined as a period of time, consecutive number years of with no change in actor's is_active or quality_class status)
    end_year INTEGER,
    -- end_year: end year of the streak
    -- (streak here is defined as a period of time, consecutive number years of with no change in actor's is_active or quality_class status)
    current_year INTEGER
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['current_year']
)