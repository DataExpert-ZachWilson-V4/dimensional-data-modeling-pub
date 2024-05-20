CREATE TABLE phabrahao.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR
    )
  ),
  -- tried using ENUM() or CONSTRAINTS but neither worked on trino/iceberg
  quality_class VARCHAR(8),
  is_active BOOLEAN,
  current_year INTEGER
) WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY ['current_year']
)
