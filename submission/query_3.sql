CREATE TABLE actors_history_scd(
  -- attribute for actor name
actor VARCHAR,
  -- attribute for actor_id identifier
actor_id VARCHAR,
  -- categorical variable based on avg. rating 
quality_class VARCHAR,
  -- flag to track if actor is active in the industry
is_active BOOLEAN,
  -- start date for actor's time frame in industry
start_date DATE,
  -- end date for actor's time frame in industry
end_date DATE,
-- partitioning key column
current_year INTEGER
)

WITH (
  -- format to staore table in Iceberg
  format = 'PARQUET',
  --  partition on current_year to optimize retrieval
  partitioning = ARRAY['current_year']
  )
  