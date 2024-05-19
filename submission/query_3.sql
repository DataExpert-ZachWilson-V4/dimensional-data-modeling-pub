--schema for type2 SCD
CREATE OR REPLACE TABLE devpatel18.actors_history_scd (
    actor   VARCHAR, 
    actor_id VARCHAR,
    quality_class VARCHAR, 
    is_active_flag INTEGER, 
    start_date INTEGER, 
    end_date INTEGER, 
    current_year INTEGER
) 
WITH
(
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)
