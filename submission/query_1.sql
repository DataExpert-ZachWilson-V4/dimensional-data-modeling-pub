CREATE OR REPLACE TABLE alia.actors (
  -- 'actor': Stores the actor's name. Part of the actor_films dataset.
  actor VARCHAR  NOT NULL,
  -- 'actor_id': Unique identifier for each actor, part of the primary key in actor_films dataset.
  actor_id VARCHAR  NOT NULL,
  -- 'films': Array of ROWs for multiple films associated with each actor. Each row contains film details.
  films ARRAY(
    ROW(
      -- 'film': Name of the film, part of actor_films dataset.
      film VARCHAR,
      -- 'votes': Number of votes the film received, from actor_films dataset.
      votes INTEGER,
      -- 'rating': Rating of the film, from actor_films dataset.
      rating DOUBLE,
      -- 'film_id': Unique identifier for each film, part of the primary key in actor_films dataset.
      film_id VARCHAR,
      -- 'year' : Release year of the film, part of actor_films dataset.
      year INTEGER
    )
  ),
  -- 'quality_class': Categorical rating based on average rating in the most recent year.
  quality_class VARCHAR,
  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  is_active BOOLEAN,
  -- 'current_year': Represents the year this row is relevant for the actor.
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )