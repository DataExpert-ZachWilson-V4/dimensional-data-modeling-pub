CREATE OR REPLACE TABLE actors (
    -- 'actor_id': primary key(unique identifier) for each actor
    actor_id VARCHAR NOT NULL,
    -- 'actor': actor's name.
    actor VARCHAR NOT NULL,
    -- 'films': Array of ROWs for multiple names of the films associated with each actor.
    films ARRAY(
        ROW(
	     -- 'film_id': Unique identifier for each film.
            film_id VARCHAR,
            -- 'film': Name of the film.
            film VARCHAR,
            -- 'votes': Number of votes the film received.
            votes INTEGER,
            -- 'rating': Rating of the film.
            rating DOUBLE,
            -- 'year': Release year of the film.
            year INTEGER
        )
    ),
    -- 'quality_class': categorical bucketing of the average rating of the movies for this actor in their most recent year.
    quality_class VARCHAR,
    -- 'is_active’: Indicates whether an actor is currently active in the film industry (i.e., making films this year).
    is_active BOOLEAN,
    -- 'current_year': The year this row represents for the actor
    current_year INTEGER
)
WITH
(
    FORMAT = 'PARQUET',
    -- Partitioned by 'current_year' for efficient time and cost-based analysis
    partitioning = ARRAY['current_year']
)

