-- Creating the 'actors' table with detailed column descriptions to increase understanding of schema

CREATE OR REPLACE TABLE vpsjul8468082.actors (
  actor VARCHAR, -- 'actor': Stores the actor's name. Part of the actor_films dataset.
  actor_id VARCHAR, -- 'actor_id': Unique identifier for each actor, part of the primary key in the actor_films dataset.
  films ARRAY(
    ROW(
      film VARCHAR, -- 'film': Name of the film, part of the actor_films dataset.
      votes INTEGER, -- 'votes': Number of votes the film received, from the actor_films dataset.
      rating DOUBLE, -- 'rating': Rating of the film, from the actor_films dataset.
      film_id VARCHAR -- 'film_id': Unique identifier for each film, part of the primary key in the actor_films dataset.
    )
  ), -- 'films': Array of ROWs for multiple films associated with each actor. Each ROW contains film details.
  quality_class VARCHAR, -- 'quality_class': Categorical rating based on the average rating in the most recent year.
  is_active BOOLEAN, -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  current_year INTEGER -- 'current_year': Represents the year this row is relevant to (e.g., the current year for the actor).
)
WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY['current_year']
)
