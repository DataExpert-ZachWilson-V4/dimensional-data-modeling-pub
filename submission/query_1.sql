CREATE OR REPLACE TABLE tharwaninitin.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(
    ROW(
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
    PARTITIONING = ARRAY['current_year']
  )