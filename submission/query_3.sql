--Query_3: DDL statement to create an actors_history_scd table for each actor in the actors table
CREATE
OR REPLACE TABLE aayushi.actors_history_scd (
    actor varchar                -- Stores the actor's name
  , quality_class varchar  -- Categorical rating based on average rating in the most recent year
  , is_active Boolean        -- Indicates if the actor is currently active
  , start_date integer       -- Marks the beginning of a particular state (quality_class/is_active)
  , end_date integer        -- Signifies the end of a particular state
  , current_year integer  -- The year this record pertains to
) 
WITH (
    FORMAT = 'PARQUET'
  , partitioning = ARRAY ['current_year'] -- Partitioned by 'current_year' for efficient time-based analysis

)
