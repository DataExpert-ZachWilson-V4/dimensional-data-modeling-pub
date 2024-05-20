--Modelling a SCD table to track historical actor data for the following fields
-- quality_class, is_active, start_date, end_date
CREATE OR REPLACE TABLE pratzo.actors_history_scd (
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning=ARRAY['current_year']
)
