-- Cumulative Table 'actors' that hold yearly details of an actor per year 
CREATE
OR REPLACE TABLE actors (
  actor VARCHAR,
  --actor: Actor name
  actor_id VARCHAR,
  -- actor_id: Actor's ID
  films ARRAY(
    ROW(
      film VARCHAR,
      -- film: The name of the film.
      votes INTEGER,
      -- votes: The number of votes the film received.
      rating DOUBLE,
      -- rating: The rating of the film.
      film_id VARCHAR,
      -- film_id: A unique identifier for each film.
      year INTEGER 
      -- year: Release year of the film
    )
  ),
  quality_class VARCHAR,
  -- quality_class: The average rating of the movies for an actor in their most recent year
  is_active BOOLEAN,
  -- is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
  current_year INTEGER -- The year this row represents for the actor
) WITH(
  format = 'PARQUET',
  partitioning = ARRAY ['current_year']
)
