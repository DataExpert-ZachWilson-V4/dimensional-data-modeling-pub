CREATE TABLE ivomuk37854.actors (
  actor_id VARCHAR,
  actor VARCHAR,
  films ARRAY(
    ROW(
      film_id VARCHAR,
      film VARCHAR,
      year INTEGER,
      votes INTEGER,
      rating DOUBLE
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

