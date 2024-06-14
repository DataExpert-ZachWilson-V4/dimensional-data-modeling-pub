CREATE TABLE changtiange199881320.actors (
    actor VARCHAR,
    actor_id VARCHAR, 
    films ARRAY(
        ROW(
            film VARCHAR, 
            votes INT, 
            rating DOUBLE, 
            film_id VARCHAR
        )
    ), 
    quality_class VARCHAR, 
    is_active BOOLEAN, 
    current_year INT
)
WITH(
    format = 'PARQUET', 
    partitioning = ARRAY['current_year']
)