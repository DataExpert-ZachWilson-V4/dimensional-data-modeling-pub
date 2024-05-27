CREATE TABLE actors_history_scd (
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER
)
WITH
  (
    FORMAT = 'PARQUET'
  )

  -- it is not specified to partition the table however this table may be accessed by year so better performance
  -- the types of the field have been taken from the actors table