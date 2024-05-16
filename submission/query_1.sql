CREATE OR REPLACE TABLE pratzo.actors (
    --  'actor': The name of the actor.
    actor VARCHAR,
    -- 'actor_id': A unique identifier for each actor.
    actor_id VARCHAR,
    -- 'films': Array containing films featuring the actor placed in a ROW. 
    films ARRAY(
        ROW(
            -- 'film': Name of the film.
            film VARCHAR,
            -- 'votes': Number of votes the film received.
            votes INTEGER,
            -- 'rating': Rating of the film.
            rating DOUBLE,
            -- 'film_id': A unique identifier for each film.
            film_id VARCHAR
        )
    ),
    -- 'quality_class': A categorical bucketing of the average rating of the movies for this actor in their most recent year.
    quality_class VARCHAR,
    -- 'is_active': Indicating if the actor is currently active.
    is_active BOOLEAN,
    -- 'current_year': The year this row represents for the actor.
    current_year INTEGER
)
WITH
    (
    -- The storage format for the table, Using parquet to efficiently store large data.
        FORMAT = 'PARQUET',
    -- Optimizing filtering queries by paritioning on 'current_year' 
        partitioning = ARRAY['current_year']
    )

