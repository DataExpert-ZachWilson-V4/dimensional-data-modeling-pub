CREATE TABLE changtiange199881320.actors_history_scd(
    actor VARCHAR,
    actor_id VARCHAR, 
    quality_class VARCHAR, 
    is_active BOOLEAN, 
    current_year INT,
    start_date INT, 
    end_date INT
)
WITH(
    format = 'PARQUET', 
    partitioning = ARRAY['current_year']
)