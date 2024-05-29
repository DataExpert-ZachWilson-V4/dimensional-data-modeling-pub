-- Create a table named 'actors' in the schema 'luiscoelho37431'
CREATE OR REPLACE TABLE luiscoelho37431.actors (
    -- Define columns names and types
    actor VARCHAR,
    actor_id VARCHAR,
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
-- Set the table options
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)