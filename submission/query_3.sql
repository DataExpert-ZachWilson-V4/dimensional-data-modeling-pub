CREATE OR REPLACE TABLE lsleena.actors_history_scd (
    actor_id VARCHAR,        -- Unique identifier for the actor_id, referenced from actors table
    quality_class VARCHAR, -- Categorical quality based on average film rating
    is_active BOOLEAN,       -- Flag to indicate if the actor is currently active in the film industry
    start_date INTEGER,      -- Start date for the validity of the recorded data, marks the beginning of a particular state
    end_date INTEGER,        -- End date for the validity of the recorded data, signifies the end of a particular state
    current_year INTEGER      -- Current year
)
WITH
  (
    FORMAT = 'PARQUET',     -- Specifies the storage format - set to PARQUET for efficient columnar storage
    partitioning = ARRAY['current_year']   -- Partitions the data by current_year to optimize query performance and data organization
  )