CREATE TABLE IF NOT EXISTS actors (
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,
  films ARRAY(
    ROW(
      film VARCHAR,
      film_id VARCHAR,
      year INTEGER,
      votes INTEGER,
      rating DOUBLE
    )
  ),
  is_active BOOLEAN,
  current_year INTEGER
)
WITH (
  format = 'parquet',
  partitioning = ARRAY['current_year']
)
;
