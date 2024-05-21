--Table creation:
--films and attributes related to it are stored as a struct
--quality_class would only handle a specific set of strings, namely 'star', 'good', 'average', 'bad'
CREATE OR REPLACE TABLE derekleung.actors (
  actor VARCHAR(63),
  actor_id VARCHAR(9),
  films ARRAY(
    ROW(
      film VARCHAR(255),
      film_id VARCHAR(9),
      votes INTEGER,
      rating DOUBLE

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
