-- This query creates or replaces the vpsjul8468082.actors_history_scd table.
-- The table is designed to implement a Type 2 Slowly Changing Dimension (SCD) to track changes in actor's quality_class and active status over time.
-- It stores information about actors, their quality classification, active status, and the period each state is valid for.
-- The table is partitioned by the current year and stored in PARQUET format for efficient querying and storage.


CREATE OR REPLACE TABLE vpsjul8468082.actors_history_scd (
  actor VARCHAR, -- 'actor': Stores the actor's name. Part of the actor_films dataset.
  quality_class VARCHAR, -- 'quality_class': Categorical rating based on average rating in the most recent year.
  is_active BOOLEAN, -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  start_date INTEGER, -- 'start_date': Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
  end_date INTEGER, -- 'end_date': Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
  current_year INTEGER -- 'current_year': The year this record pertains to. Useful for partitioning and analyzing data by year.
)
WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY['current_year']
)
