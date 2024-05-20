-- Creates a versioned history table for actors to track changes over time using Type 2 SCD model
CREATE OR REPLACE TABLE jlcharbneau.actors_history_scd
(
  actor VARCHAR,          -- Name of the actor
  actor_id VARCHAR,       -- Unique identifier for the actor, primary key
  quality_class VARCHAR,  -- Categorization of the actor based on average movie ratings
  is_active BOOLEAN,      -- Indicator if the actor is currently active
  start_date DATE,        -- Start date of the record validity
  end_date DATE,          -- End date of the record validity, NULL if currently valid
  current_year INTEGER    -- Year of the record, used for partitioning
)
WITH
(
  FORMAT = 'PARQUET',
  PARTITIONING = ARRAY['current_year']
)