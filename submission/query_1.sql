-- Creating a new table named "nancyatienno21998.actors"
CREATE TABLE nancyatienno21998.actors(
  actor VARCHAR,                               -- Column to store actor names
  actor_id VARCHAR,                            -- Column to store actor IDs
  films ARRAY(                                 -- Column to store an array of films for each actor
    ROW(                                       -- Nested structure for each film
      year INTEGER,                            -- Year of the film
      film VARCHAR,                            -- Name of the film
      votes INTEGER,                           -- Number of votes for the film
      rating DOUBLE,                           -- Rating of the film
      film_id VARCHAR                          -- ID of the film
    )
  ),
  quality_class VARCHAR,                       -- Column to store the quality class of the actor
  is_active BOOLEAN,                           -- Column to store whether the actor is active
  current_year INTEGER                         -- Column to store the current year
)
WITH (
  FORMAT = 'PARQUET',                          -- Specifying the storage format as Parquet
  partitioning = ARRAY['current_year']         -- Defining partitioning by the 'current_year' column
)
