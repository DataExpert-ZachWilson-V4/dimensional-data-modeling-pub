-- Third Query
CREATE TABLE 
    actors_history_scd(
    actor_id VARCHAR,
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER,
    start_date DATE,
    end_date DATE
    
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
)