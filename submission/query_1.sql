CREATE OR REPLACE TABLE tdueck66966.actors ( --create or replace the actors table.
  actor VARCHAR, --'actor': Stores the actor's name. Part of the actor_films dataset.
  actor_id VARCHAR, --'actor_id': Stores the actor's unique identification number from the actor_films dataset. Used to distinguish actors with the same name.
  films ARRAY( --'film': array of films for better compression
    ROW(
      year INT, --'year': Stores the film's year, which is helpful to unnest the array later on. Part of the actor_films dataset.
      film VARCHAR, --'film': Stores the film's name. Part of the actor_films dataset.
      votes INT, --'votes': Stores the votes cast for the film. Part of the actor_films dataset.
      rating DOUBLE, --'rating': Stores the rating for the film. Part of the actor_films dataset.
      film_id VARCHAR --'film_id': Stores the film's unique identification number from the actor_films dataset. Used to distinguish films with the same name.
    )
  ),
  quality_class VARCHAR, --'quality_class': Stores the quality class which is a calculated text value describing the average ratings for the actor's films per year.
  is_active BOOLEAN, --'is_active': Stores the true/false indicator when an actor was in at least one film per year.
  current_year INT --'current_year': Stores the current year as part of cumulative table design for quick lookup to compare current year with historical values.
)
WITH(
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
)
