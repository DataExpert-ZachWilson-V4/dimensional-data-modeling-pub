--query_3
/* DDL to create actors_history_scd table */

CREATE OR REPLACE TABLE actors_history_scd (
  actor VARCHAR(255), -- Stores the actor's name. 
  actor_id VARCHAR(255),  -- Unique identifier for each actor.
  quality_class VARCHAR, -- Categorical rating based on average rating in the most recent year.
  is_active BOOLEAN,  -- Indicates if the actor is currently active, based on making films this year.
  start_date INTEGER,  -- Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
  end_date INTEGER,    -- Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
  current_year INTEGER  -- The year this record pertains to. Useful for partitioning and analyzing data by year.
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']  
)
