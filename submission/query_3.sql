--Query 3
--Creating SCD table for each actor in table
create table hariomnayani88482.actors_history_scd 
(
  actor varchar,
  quality_class varchar,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
