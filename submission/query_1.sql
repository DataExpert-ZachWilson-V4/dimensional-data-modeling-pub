CREATE
OR REPLACE TABLE barrocaeric.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR,
      -- Adding year even when it has not been specified since it seems necessary
      year INTEGER
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
  )