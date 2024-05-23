CREATE OR REPLACE TABLE ttian45759.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(
    Row(
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR,
      year INTEGER
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