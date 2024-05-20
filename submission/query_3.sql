/*
Actors History SCD Table DDL (query_3)

Write a DDL statement to create an actors_history_scd table that tracks the following fields for each actor in the actors table:
  * quality_class
  * is_active
  * start_date
  * end_date
Note that this table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (start_date and end_date).
*/


CREATE TABLE harathi.actors_history_scd (
  actor_id VARCHAR,
  actor VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY ['current_year']
)
