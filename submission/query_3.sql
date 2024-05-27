CREATE OR REPLACE TABLE iliamokhtarian.actors_history_scd (
    -- 'actor': Stores the actor's name.
    actor VARCHAR NOT NULL,
    -- 'actor_id': Unique identifier for each actor.
    actor_id VARCHAR NOT NULL,
    -- 'quality_class': Categorical rating based on average rating.
    quality_class VARCHAR NOT NULL,
    -- 'is_active': Indicates if the actor is currently active.
    is_active BOOLEAN NOT NULL,
    -- 'start_date': Start date of the record's validity period.
    start_date DATE NOT NULL,
    -- 'end_date': End date of the record's validity period.
    end_date DATE,
    -- 'current_year': The year this record is relevant for.
    current_year INTEGER NOT NULL
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)
