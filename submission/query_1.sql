CREATE TABLE actors (
  -- create actors table in Iceberg
  -- Name of the actor defined as varchar
  actor VARCHAR,
  -- actor's identifier defined as varchar
  actor_id VARCHAR,
  /* array of struct collections of all attributes of a movie for a actor */
  films ARRAY(ROW(
      YEAR INTEGER,
      film VARCHAR,
      votes BIGINT,
      rating DOUBLE,
      film_id VARCHAR
    )),
  /* categorical variable for average rating of a film in the most recent year */
  quality_class VARCHAR,
  -- boolean indicating if the actor is currently active
  is_active BOOLEAN,
  -- this year record
  current_year INTEGER
)
/* define format of table in Iceberg and partition column */
WITH
  (
    -- storing table as a parquet file
    FORMAT = 'PARQUET',
    -- partitioning the file by current_year
    partitioning = ARRAY['current_year']
  )
  