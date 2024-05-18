CREATE OR REPLACE TABLE actors
(
    actor VARCHAR NOT NULL,    -- Name of the actor from the actor dataset
    actor_id VARCHAR NOT NULL, -- Unique identifier for each actor from actor dataset
    films ARRAY
    (
        ROW
        (
              film VARCHAR,    -- Film in which this actor performed
              year INTEGER,    -- The year of film production
              votes INTEGER,   -- Number of votes this film recieved
              rating DOUBLE,   -- Rating given to the film by the audience
              film_id VARCHAR  -- Unique identifier for each film
        )
    ),
    quality_class VARCHAR,     -- This is a categorical bucketing given to each actor based on the average rating of the film for the current year
    is_active BOOLEAN,         -- This field indicates if the actor is currently active in the film industry
    current_year INTEGER NOT NULL     -- Represents the year this row is relevant for the actor
)
WITH
(
  FORMAT = 'PARQUET',                   -- We will save the data in PARQUET format on write to get best commpressed dataset
  partitioning = ARRAY['current_year']  --  partition on write for current year
)
