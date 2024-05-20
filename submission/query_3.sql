CREATE
OR REPLACE TABLE  halloweex.actors_history_scd (
    actor VARCHAR NOT NULL,
    quality_class VARCHAR,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
    is_current BOOLEAN,
    is_active BOOLEAN
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)