--Q3 DDL statement to create the actors_history_scd table
CREATE OR REPLACE TABLE sravan.actors_history_scd (
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER
)
WITH (
  FORMAT = 'PARQUET',
  PARTITIONING = ARRAY ['current_year']  -- Partitioned by 'current_year' for efficient time-based analysis
)
