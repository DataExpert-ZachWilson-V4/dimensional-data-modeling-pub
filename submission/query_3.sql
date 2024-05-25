CREATE OR REPLACE TABLE akp389.actors_history_scd (
  -- 'actor': Stores the actor's name. Part of the actor_films dataset.
  actor VARCHAR,
  -- 'actor_id' : Stores the actor's actor id. Part of the actor_films dataset.
  actor_id VARCHAR,
  -- 'quality_class': Categorical rating based on average rating in the most recent year
  quality_class VARCHAR,
  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  is_active BOOLEAN,
  -- 'start_date': Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
  start_date INTEGER,
  -- 'end_date': Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
  end_date INTEGER,
  -- 'current_year': The year this record pertains to. Useful for partitioning and analyzing data by year.
  current_year INTEGER 
) WITH (
  -- Data stored in PARQUET format for optimized analytics.
  FORMAT = 'PARQUET',
  -- Partitioned by 'current_year' for efficient time-based analysis.
  partitioning = ARRAY ['current_year'] 
)