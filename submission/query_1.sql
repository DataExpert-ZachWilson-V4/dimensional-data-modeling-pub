CREATE OR REPLACE TABLE raviks90.actors (
    actor VARCHAR, -- name of the actor
    actor_id VARCHAR, -- unique id of the actor
    films ARRAY(      -- array of film attributes of actor for each year
        ROW(
            film_id VARCHAR,
            film VARCHAR,
            votes INTEGER,
            rating DOUBLE,
            year INTEGER
        )
    ),
    quality_class VARCHAR, --quality class derived based on Avg rating
    is_active BOOLEAN, -- if actor is still acting in the current year
    current_year INTEGER -- gives as of latest year data
)
WITH
    (
        format = 'PARQUET',
        partitioning = ARRAY['current_year'] -- partition key
    )
