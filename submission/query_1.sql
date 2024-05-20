CREATE OR REPLACE TABLE sagararora492.actors --specify dataset and table name.
(
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY( -- this is a array
    ROW( -- this is how we are defining a struct
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
) WITH (
  format = 'PARQUET', 
  partitioning = ARRAY['current_year']
)