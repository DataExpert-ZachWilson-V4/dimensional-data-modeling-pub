
-- Create table
CREATE OR REPLACE TABLE raniasalzahrani.actors (
    actor_id INT,
    actor VARCHAR,
    films ARRAY(
        ROW(
            film VARCHAR,
            votes INT,
            rating DOUBLE,
            film_id INT
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
