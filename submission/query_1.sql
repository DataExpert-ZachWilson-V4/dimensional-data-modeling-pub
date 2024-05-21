-- Creating table actors from the fields in actors_films dataset

CREATE OR REPLACE TABLE shruthishridhar.actors (
    actor VARCHAR,  -- actor's name
    actor_id VARCHAR, -- actor's unique identifier
    films ARRAY(  -- actor's films array with a struct for each film
      ROW(
        film VARCHAR, -- film name
        votes INTEGER,  -- number of votes this film received
        rating DOUBLE,  -- rating for the film
        film_id VARCHAR, -- film's unique identifier
        year INTEGER  -- year in which the film released
      )
    ),
    quality_class VARCHAR,  -- bucketing of average rating of films the most recent year
    is_active BOOLEAN,  -- indicates if the actor is currently active
    current_year INTEGER  -- the year this row represents for this actor
)
WITH (
    format = 'PARQUET', -- setting data format as PARQUET
    partitioning = ARRAY['current_year']  -- partitioning this data based on current_year
)