CREATE OR REPLACE TABLE jsgomez14.actors_history_scd
(
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_year INTEGER,
  end_year INTEGER
) WITH (
  format = 'PARQUET', --specify the format of the table.
  partitioning = ARRAY['start_year'] 
  -- Specify the partitioning column.
  -- To take advantage of Parquet's run length encoding 
  -- and compress repetitive data to save disk space.
)