/* DDL statement to create an actors_history_scd table that tracks the relevant fields */

CREATE TABLE actors_history_scd (
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,
  -- is_active field to track if actor is active in the industry
  is_active BOOLEAN,
  -- Type 2 SCD to track beginning of actors active state
  start_date INTEGER,
  -- Type 2 SCD to track the ending of actors active state
  end_date INTEGER,
  current_year INTEGER
)

WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
) 