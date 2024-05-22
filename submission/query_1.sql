--Table creation saidaggupati.actors
CREATE or REPLACE TABLE saidaggupati.actors (

-- actor: Actor name
    actor VARCHAR, 
     
--actor_id: Actor's ID
    actor_id VARCHAR,
    
--films: An array of struct with the following fields:
--film: The name of the film.
--votes: The number of votes the film received.
--rating: The rating of the film.
--film_id: A unique identifier for each film.

    films ARRAY(
    ROW(
    film VARCHAR,
    votes INTEGER,
    rating DOUBLE,
    film_id VARCHAR
  )
),

--quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year.

  quality_class VARCHAR,
  
--is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

  is_active INTEGER,
  
--current_year: The year this row represents for the actor

  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
