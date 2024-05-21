CREATE TABLE IF NOT EXISTS andreskammerath.actors (
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY(
        ROW(
            film VARCHAR,
            votes BIGINT,
            rating DOUBLE,
            film_id VARCHAR
        )
    ),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INT
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)