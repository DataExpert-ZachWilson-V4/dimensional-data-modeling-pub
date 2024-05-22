-- First SQL command
CREATE TABLE actors (
    actor_id VARCHAR,
    actor VARCHAR,
    films ARRAY(ROW(
        film VARCHAR,
        year INTEGER,
        votes INTEGER,
        rating DOUBLE,
        film_id VARCHAR
    )),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)



