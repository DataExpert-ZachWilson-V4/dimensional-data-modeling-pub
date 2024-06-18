CREATE TABLE changtiange199881320.actors (
    actor_id VARCHAR,
    actor VARCHAR, 
    films ARRAY(
        ROW(
            film_id VARCHAR,
            film VARCHAR, 
            votes INT, 
            rating DOUBLE, 
            year INT
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