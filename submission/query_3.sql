--Q3 DDL statement to create the actors_history_scd table
CREATE OR REPLACE TABLE sravan.actors_history_scd (
  actor VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH (
  FORMAT = 'PARQUET',
  PARTITIONING = ARRAY ['current_year'] 
)
