CREATE TABLE actors (
    -- 'actor': Stores the actor's name.
    actor VARCHAR,
    -- 'actor_id': A unique identifier for each actor.
    actor_id VARCHAR,
    -- 'films' : Array of structs containing rows which describe film data.
    films ARRAY(
        ROW(
            -- 'year': Year a film was released.
            year INTEGER,
            -- 'film': Name of the film.
            film VARCHAR,
            -- 'votes': The number of votes the film received.
            votes INTEGER,
            -- 'rating' : The rating of the film.
            rating DOUBLE,
            -- 'film_id' : A unique identifier for each film.
            film_id VARCHAR
        )
    ),
    -- 'quality_class': A categorical bucketing of the average rating of the movies for an actor in their most recent eyar.
    quality_class VARCHAR,
    -- 'is_active': Indicated whether an actor is currently active in the film industry.
    is_active BOOLEAN,
    -- 'current_year': The year this row represents for the actor.
    current_year INTEGER
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['current_year']
)
