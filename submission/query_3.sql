CREATE OR REPLACE TABLE mymah592.actors_history_scd (
    actor VARCHAR,
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    Start_year INTEGER,
    End_year   INTEGER,
    current_year INTEGER
)
WITH (
    format ='PARQUET',
partitioning = ARRAY['current_year']
)
