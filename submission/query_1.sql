CREATE OR REPLACE TABLE jb19881.actors (
    actor varchar COMMENT 'Stores the actor''s name',
    actor_id varchar NOT NULL COMMENT 'Unique identifier for each actor, part of the primary key in actor_films dataset.',
    films array(
        row(
            -- 'film': Name of the film
            film varchar,
            -- 'votes': Number of votes the film received
            votes integer,
            -- 'rating': Rating of the film
            rating double,
            -- 'film_id': Unique identifier for each film, part of the primary key in actor_films dataset.
            film_id varchar
        )
    ) COMMENT 'Array of ROWs for multiple films associated with each actor. Each row contains film details.',
    quality_class varchar COMMENT 'Categorical rating based on average rating in the most recent year. See query_2.sql for categories',
    is_active boolean COMMENT 'An actor is active if they have at least one film in the year',
    current_year integer COMMENT 'Represents the year this row is relevant for the actor.'
)
COMMENT 'Data is sourced from the actor_films dataset. The actors table has one row per actor per year and is partitioned by current_year'
WITH
    (
        -- The Parquet file format is used to optimize for analytical query loads
        format = 'PARQUET',
        -- Partitioned by 'current_year' for efficient time-based data processing and analysis.
        partitioning = ARRAY['current_year']
    )