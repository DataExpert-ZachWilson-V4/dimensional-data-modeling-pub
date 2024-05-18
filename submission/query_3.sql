-- Creating a new table named "nancyatienno21998.actors_history_scd"
CREATE TABLE nancyatienno21998.actors_history_scd (
  -- Defining columns for the table
  actor VARCHAR,                    -- Column to store actor names
  quality_class VARCHAR,           -- Column to store quality class
  is_active BOOLEAN,               -- Column to store whether the actor is active
  start_date INTEGER,              -- Column to store the start date of an actor's
  end_date INTEGER,                -- Column to store the end date of an actor's record
  current_year INTEGER             -- Column to store the current year
)

WITH (
  format = 'PARQUET',             -- Specifying the storage format as Parquet
  partitioning = ARRAY['current_year']  -- Defining partitioning by the 'current_year' column
)
