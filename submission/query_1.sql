-- Create actor table which contains data about an actor, the films they starred in, quality class and active status across different years

CREATE OR REPLACE TABLE ovoxo.actors (
  actor VARCHAR, -- stores the actor's name
  actor_id VARCHAR, -- stores the actor's idenifier. Unique identifier of actor
  films ARRAY( -- array that contains rows of film details for each actor. Contains multiple films across multiple years
    ROW(
      year INTEGER, -- release year of film, adding this column inprove usabilty of the data for analytics downstream
      film VARCHAR, -- name of file
      votes INTEGER, -- number of votes film received
      rating DOUBLE, -- film rating
      film_id VARCHAR -- film identifier
    )
  ),
  quality_class VARCHAR, -- categorical rating of film. This is based on most recent year's average film rating.
  is_active BOOLEAN, -- True if actor is active in current_year ie did actor release a movie in current_year
  current_year INTEGER -- year relevant to record
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )