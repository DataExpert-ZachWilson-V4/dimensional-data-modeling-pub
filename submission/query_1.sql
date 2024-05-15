CREATE TABLE actors (
    actor VARCHAR,
    actor_id VARCHAR,
    -- array of rows for each film up to current_year
    films ARRAY(
        ROW(
            film VARCHAR,
            votes INTEGER,
            rating DOUBLE,
            film_id VARCHAR
        )
    ),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)
WITH
    (
        format = 'PARQUET',
        partitioning = ARRAY['current_year']
    )