CREATE OR REPLACE TABLE jsgomez14.actors --specify dataset and table name.
(
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY( --films is an array of structs.
    ROW( -- ROW permits to define a struct.
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
  format = 'PARQUET', --specify the format of the table.
  partitioning = ARRAY['current_year'] --specify the partitioning column. 
  -- To take advantage of Parquet's run length encoding 
  -- and compress repetitive data to save disk space.
)