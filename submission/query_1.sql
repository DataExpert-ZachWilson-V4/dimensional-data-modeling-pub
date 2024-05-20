CREATE TABLE actors (
  actor VARCHAR,
  actor_id VARCHAR,

  films ARRAY(
    ROW(
      year INTEGER,
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )