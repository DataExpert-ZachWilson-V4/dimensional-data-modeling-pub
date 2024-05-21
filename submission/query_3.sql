CREATE OR REPLACE TABLE actors_history_scd (
  -- 'actor': Stores the actor's name.
  actor VARCHAR,
  -- 'quality_class': Categorical rating based on average rating in the most recent year.
  quality_class VARCHAR,
  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  is_active BOOLEAN NOT NULL,
  -- 'start_date': Marks the beginning of a particular state.
  start_date INTEGER,
  -- 'end_date': Signifies the end of a particular state. 
  end_date INTEGER,
  -- 'current_year': The year this record pertains to.
  current_year INTEGER 
) WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY ['current_year'] 
)
