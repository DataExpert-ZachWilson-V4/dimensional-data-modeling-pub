-- DDL for table to store information about actors
CREATE OR REPLACE TABLE saismail.actors (
  -- Name of the actor
  actor VARCHAR,
  -- Unique identifier for the actor
  actor_id VARCHAR,
  -- Array of films the actor has appeared in, with additional details
  films ARRAY (
    ROW (
      -- Release year of the film
      "year" INTEGER,
      -- Name of the film
      film VARCHAR,
      -- Number of votes the film has received
      votes INTEGER,
      -- Rating of the film
      rating DOUBLE,
      -- Unique identifier for the film
      film_id VARCHAR
    )
  ),
  -- Class representing the quality of the actor (e.g., "good", "bad", "average", "star")
  quality_class VARCHAR,
  -- Indicates whether the actor is currently active
  is_active BOOLEAN,
  -- The current year (used for partitioning)
  current_year INTEGER
)
-- Specify the format of the table data as Parquet
WITH (
  format = 'PARQUET',
  -- Define partitioning by the current_year column
  partitioning = ARRAY['current_year']
)
