CREATE TABLE billyswitzer.actors_history_scd
(
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,  
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE,
  current_year INTEGER
)
WITH 
(
  FORMAT = 'PARQUET',
  PARTITIONING = ARRAY['current_year']
)
