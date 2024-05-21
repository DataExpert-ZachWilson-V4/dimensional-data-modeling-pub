-- Table for SCD query_3
CREATE TABLE IF NOT EXISTS andreskammerath.actors_history_scd (
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INT,
    end_date INT
)
WITH(
    FORMAT = 'PARQUET',
    partitioning = ARRAY['start_date']
)