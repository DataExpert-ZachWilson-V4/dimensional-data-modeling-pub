CREATE TABLE jesusr93.actors (
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY(
        ROW(
            film VARCHAR,
            votes BIGINT,
            rating DOUBLE,
            film_id VARCHAR,
            year INTEGER -- I added the year becasue one actor can have more than one film in a year
        )
    ),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)
WITH (FORMAT = 'PARQUET',partitioning = ARRAY['current_year'])