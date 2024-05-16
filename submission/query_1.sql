--Table creation:
--films and attributes related to it are stored as a struct
--quality_class would only handle a specific set of strings, namely 'star', 'good', 'average', 'bad'
CREATE TABLE derekleung.actors (
  actor VARCHAR(63),
  actor_id CHARACTER(9),
  films ARRAY(
    ROW(
      film INTEGER,
      votes INTEGER,
      rating DOUBLE,
      film_id CHARACTER(9)
    )
  ),
  quality_class VARCHAR(7),
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
