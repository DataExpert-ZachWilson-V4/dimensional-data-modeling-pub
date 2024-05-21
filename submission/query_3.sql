--Q3 DDL statement to create the actors_history_scd table
CREATE OR REPLACE TABLE sravan.actors_history_scd (
  actor VARCHAR,            -- Stores the actor's name
  quality_class VARCHAR,    -- Categorical rating based on average rating in the most recent year
  is_active BOOLEAN,        -- Indicates if the actor is currently active
  start_date INTEGER,       -- Marks the beginning of a particular state (quality_class/is_active)
  end_date INTEGER,         -- Signifies the end of a particular state
  current_year INTEGER      -- The year this record pertains to
)
WITH (
  FORMAT = 'PARQUET',
  PARTITIONING = ARRAY ['current_year']  -- Partitioned by 'current_year' for efficient time-based analysis
)
