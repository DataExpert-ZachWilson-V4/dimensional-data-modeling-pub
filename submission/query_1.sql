-- This DDL Create Query creates a table names ACTORS in our schema
-- This table is designed to store information about actors and the films they have performed in.

CREATE OR REPLACE TABLE actors
(
    actor VARCHAR NOT NULL,    -- Name of the actor from the actor dataset
    actor_id VARCHAR NOT NULL, -- Integer id for each actor from actor dataset
    films ARRAY
    (
        ROW
        (
              film VARCHAR,    -- Film in which this actor performed 
              year INTEGER,    -- The year of film production is represented as an integer due to the format of the raw data.
              votes INTEGER,   -- Number of votes this film recieved 
              rating DOUBLE,   -- Rating given to the film by the audience
              film_id VARCHAR  -- Integer id is given to each film
        )
    ),
    quality_class VARCHAR,     -- This is a categorical bucketing given to each actor based on the average rating of the film for the current year
    is_active BOOLEAN,         -- This filed incdicates if the actor is still active in the film industry if the current year = the year of his last film
    years_since_last_active INTEGER,  -- Derived field which shows a difference between (current_year - year of last film)
    current_year INTEGER NOT NULL     -- system's current year in YYMMDD format and hence integer 
)
WITH
(
  FORMAT = 'PARQUET',                   -- We will save the data in PARQUET format on write to get best commpressed dataset
  partitioning = ARRAY['current_year']  -- And for run-length encoding to work at it's best we need to partiiton on write for current year to create folders and narrow the search for efficiency
)