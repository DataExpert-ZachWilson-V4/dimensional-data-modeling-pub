--create actors table 
create or replace table vaishnaviaienampudi83291.actors(
  actor_id VARCHAR,
  actor VARCHAR,
  films ARRAY( -- create films array  
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR
    )),
    quality_class VARCHAR, 
    is_active BOOLEAN,
    current_year INTEGER
)
WITH (format = 'PARQUET',
       partitioning = ARRAY['current_year'])
