/*
Actors Table DDL (query_1)
prompt: 
Write a DDL query to create an actors table with the following fields:
* actor: Actor name
* actor_id: Actor's ID
* films: An array of struct with the following fields:
    * film: The name of the film.
    * votes: The number of votes the film received.
    * rating: The rating of the film.
    * film_id: A unique identifier for each film.
* quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
    * star: Average rating > 8.
    * good: Average rating > 7 and ≤ 8.
    * average: Average rating > 6 and ≤ 7.
    * bad: Average rating ≤ 6.
* is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
* current_year: The year this row represents for the actor

**/
   
CREATE OR REPLACE TABLE harathi.actors (
  --actor: Actor name
   actor VARCHAR,
   --actor_id: Actor's ID
  actor_id VARCHAR,
  -- films: An array of struct with the following fields:
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
