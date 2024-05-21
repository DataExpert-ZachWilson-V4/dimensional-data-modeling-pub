CREATE OR REPLACE TABLE tdueck66966.actors_history_scd (
  actor_id VARCHAR, --'actor_id': Stores the actor's unique identification number from the actor_films dataset. Used to distinguish actors with the same name.
  actor VARCHAR, --'actor': Stores the actor's name. Part of the actor_films dataset.
  quality_class VARCHAR, --'quality_class': Stores the quality class which is a calculated text value describing the average ratings for the actor's films per year.
  is_active BOOLEAN, --'is_active': Stores the true/false indicator when an actor was in at least one film per year.
  start_date INT, --'start_date': Stores the start year for the actor's activity and quality class streak as calculated for the scd.
  end_date INT, --'end_date': Stores the end year for the actor's activity and quality class streak as calculated for the scd.
  current_year INT --'current_year': Stores the current year as part of cumulative table design for quick lookup to compare current year with historical values.
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
)
