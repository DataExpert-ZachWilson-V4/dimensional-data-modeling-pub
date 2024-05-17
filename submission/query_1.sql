CREATE TABLE juliescherer.actors (
  actor VARCHAR,
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
) WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY ['current_year']
)