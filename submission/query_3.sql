CREATE TABLE bgar.actors_history_scd (
  actor VARCHAR,
  quality_class VARCHAR,  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  is_active BOOLEAN,  -- 'is_active': Indicates if the actor is currently active, based on making films this year.
  start_date INTEGER,   -- 'start_date': Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
  end_date INTEGER,  -- 'end_date': Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
  current_year INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']
)
