CREATE TABLE changtiange199881320.actors_history_scd(
    actor VARCHAR, 
    quality_class VARCHAR, 
    is_active BOOLEAN, 
    start_date INT, 
    end_date INT, 
    current_year INT -- partition key
)
WITH(
    format = 'PARQUET', 
    partitioning = ARRAY['current_year']
)