CREATE TABLE nattyd.actors_history_scd (
    actor VARCHAR,
    actorid VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)
