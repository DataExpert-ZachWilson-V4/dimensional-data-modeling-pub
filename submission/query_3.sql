-- DDL statement to create an actors_history_scd table that tracks the following fields for each actor in the actors table
CREATE OR REPLACE TABLE actors_history_scd(
    actor VARCHAR,
    quality_class VARCHAR, -- Categorical rating based on average rating in the most recent year.
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)