-- Create a table named 'actors' in the schema 'martinaandrulli' or replace it if already exists
CREATE OR REPLACE TABLE martinaandrulli.actors (
    actor VARCHAR,
    actor_id VARCHAR NOT NULL,
    -- Define a column named 'films' of type ARRAY containing rows with film-related information
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
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year'] -- Partitioning data based on the year to improve queries based on year 
)