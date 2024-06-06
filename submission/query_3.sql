--Actors History SCD Table DDL (query_3)
--Write a DDL statement to create an actors_history_scd table that tracks the following fields for each actor in the actors table.

CREATE or REPLACE TABLE saidaggupati.actors_history_scd(
       actor VARCHAR,
       quality_class VARCHAR,
       is_active BOOLEAN,
-- specified in the prompt (table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (start_date and end_date))
       start_date DATE,
       end_date DATE,
--partition key - please refer to partitioning piece in the code.
       current_year INTEGER 
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
