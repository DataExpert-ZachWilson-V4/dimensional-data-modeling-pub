-- Type 2 Slowly Changing Dimension Table to track specified fields for each actor in the actors table
CREATE 
OR REPLACE TABLE mariavyso.actors_history_scd (
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH
    (
        FORMAT = 'PARQUET',
        partitioning = ARRAY['current_year']
    )
