-- Create SCD table for erich.actors data tracking changes for the
-- quality_class and is_active columns.
CREATE TABLE erich.actors_history_scd (
    actor_id VARCHAR,
    -- 'actor': Stores the actor's name. Part of the actor_films dataset.
    actor VARCHAR,
    -- 'quality_class': Categorical rating based on average rating in the most recent year.
    quality_class VARCHAR,
    -- 'is_active': Indicates if the actor is currently active, based on making films this year.
    is_active BOOLEAN,
    -- 'start_date': Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
    start_date INTEGER,
    -- 'end_date': Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
    end_date INTEGER,
    -- 'current_year': The year this record pertains to. Useful for partitioning and analyzing data by year.
    current_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)