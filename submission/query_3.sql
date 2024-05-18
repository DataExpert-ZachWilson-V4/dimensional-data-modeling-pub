-- Create the actors_history_scd table 
CREATE OR REPLACE TABLE actors_history_scd (
    actor VARCHAR,           -- actor name 
    actor_id VARCHAR,        -- actor id as unique identifier
    quality_class VARCHAR,   -- Categorical quality based on average film rating
    is_active BOOLEAN,       -- Flag to indicate if the actor is currently active in the film industry
    start_date INTEGER,      -- Start date and keeping it Intger to match the current_year column's datatype
    end_date INTEGER,        -- End date and keeping it Intger to match the current_year column's datatype
    current_year INTEGER     -- For partitioning stratergy
)
WITH
  (
    FORMAT = 'PARQUET',     
    partitioning =         
    ARRAY['current_year']   -- Partitioning on current_year for query efficiency 
  )