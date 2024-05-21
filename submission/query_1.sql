CREATE TABLE IF NOT EXISTS dennisgera.actors (
    -- 'actor': Stores the actor's name.
    actor VARCHAR, 
    -- 'actor_id': Stores the actor's unique identifier.
    actor_id VARCHAR,
    -- 'films': Stores the films the actor has participated in.
    films ARRAY(
        ROW(
            film VARCHAR,
            votes INTEGER,
            rating DOUBLE,
            film_id VARCHAR
        )
    ),
    -- 'quality_class': Stores the quality class of the actor.
    quality_class VARCHAR,
    -- 'is_active': Stores whether the actor is active or not.
    is_active BOOLEAN,
    -- 'current_year': Stores the current year.
    current_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)
