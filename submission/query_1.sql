/*
DDL query to create an actors table with the following fields:

actor: Actor name
actor_id: Actor's ID
films: An array of struct with the following fields:
  film: The name of the film.
  votes: The number of votes the film received.
  rating: The rating of the film.
  film_id: A unique identifier for each film.
quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
current_year: The year this row represents for the actor
*/

CREATE OR REPLACE TABLE actors (
  -- actor: stores the actor's name
  actor VARCHAR,
  -- actor_id: unique identifier for each actor
  actor_id VARCHAR,
  -- films: array with the actor's film details
  films ARRAY(
    ROW(
      -- year: year that the film was released
      year INTEGER,
      -- film: nthe name of the film
      film VARCHAR,
      -- votes: number of votes the film received
      votes INTEGER,
      -- rating: the rating of the film
      rating DOUBLE,
      -- film_id: unique identifier for each film
      film_id VARCHAR
    )
  ),
  -- quality class: a categorical bucketing of the average rating of the movies for this actor in their most recent year
  quality_class VARCHAR,
  -- is_active: indicates whether an actor is currently active in the film industry
  is_active BOOLEAN,
  -- current_year: the year this row represents for the actor
  current_year INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
  )
