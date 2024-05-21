CREATE TABLE jesusr93.actors_history_scd (
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    current_year INTEGER
)
WITH (format = 'PARQUET', partitioning = ARRAY['current_year'])