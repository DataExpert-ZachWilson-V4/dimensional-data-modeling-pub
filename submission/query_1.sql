CREATE or REPLACE TABLE nonasj.actors (
  --actor: Actor name
   actor VARCHAR,
   --actor_id: Actor's ID
  actor_id VARCHAR,
  -- films: An array of struct with the following fields:
  films ARRAY(
    ROW(
      film VARCHAR, -- film: The name of the film.
      film_id VARCHAR, --film_id: A unique identifier for each film.
      year INTEGER, -- Year this grain belongs to
      votes INTEGER, -- votes: The number of votes the film received.
      rating DOUBLE -- rating: The rating of the film.
    )
  ),
 --quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year
  quality_class varchar,
   --is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry
  is_active BOOLEAN,
   --current_year: The year this row represents for the actor
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year'])