-- This Query creates an empty table for Actors
CREATE OR REPLACE TABLE mymah592.Actors (
    actor VARCHAR,
    actor_ID VARCHAR,
    films ARRAY<ROW(Year INTEGER, film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR)>,
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)    
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)

