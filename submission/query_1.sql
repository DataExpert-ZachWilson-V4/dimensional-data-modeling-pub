CREATE OR REPLACE TABLE sravan.actors (
    actor VARCHAR,       -- The name of the actor
    actor_id VARCHAR,    -- A unique identifier for the actor
    films ARRAY(         -- An array of structs representing the films the actor has worked in
        ROW(
            film VARCHAR,    -- The name of the film
            votes INTEGER,   -- The number of votes the film has received
            rating DOUBLE,   -- The rating of the film
            film_id VARCHAR  -- A unique identifier for the film
        )
    ),
    quality_class VARCHAR,  -- A categorization of the actor's films based on their average rating
    is_active BOOLEAN,      -- A flag indicating if the actor is currently active in the film industry (making films this year)
    current_year INTEGER    -- The current year for which the actor's data is recorded
)
WITH (
    FORMAT = 'PARQUET',    -- The table will be stored in Parquet format
    partitioning = ARRAY['current_year'] -- The table will be partitioned by the 'current_year' column
)
