-- ### Actors History SCD Table DDL (query_3)
--
-- Write a DDL statement to create an `actors_history_scd` table that tracks the following fields for each actor in the `actors` table:
--
-- - `quality_class`
-- - `is_active`
-- - `start_date`
-- - `end_date`
--
-- Note that this table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (`start_date` and `end_date`).

create or replace table shababali.actors_history_scd (
    actor varchar,
    actor_id varchar,
    quality_class varchar,
    is_active boolean,
    start_date integer,
    end_date integer,
    current_year integer
) with (
    format = 'PARQUET',
    partitioning = array['current_year']
)
