-- aggregate table of movies an actor has appeared in each year. (1 row per actor)
CREATE OR REPLACE TABLE actors (
  actor VARCHAR(255),
  actor_id VARCHAR(255),
  -- 'films': Array of ROWs for multiple films associated with each actor. Each row contains film details.
  films ARRAY(
    ROW(
      -- 'film': Name of the film.
      film VARCHAR(255),
      -- 'votes': Number of votes the film received.
      votes INTEGER,
      -- 'rating': Rating of the film.
      rating DOUBLE,
      -- 'film_id': Unique identifier for each film.
      film_id VARCHAR(255),
      -- 'year': Release year of the film.
      year INTEGER
    )
  ),
  -- 'quality_class': Categorical rating based on average rating in the most recent year.
  quality_class VARCHAR(255),
  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  is_active BOOLEAN,
  -- 'current_year': Represents the year this row is relevant for the actor.
  current_year INTEGER
)
WITH(
  -- Data stored in PARQUET format for optimized analytics.
  format='PARQUET',
  -- Partitioned by 'current_year' for efficient time-based analysis.
  partitioning=array['current_year']
)
