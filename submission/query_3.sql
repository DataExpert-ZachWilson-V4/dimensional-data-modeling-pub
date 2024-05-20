/*
Write a DDL statement to create an `actors_history_scd` table that tracks the following fields for each actor in the `actors` table:

- `quality_class`
- `is_active`
- `start_date`
- `end_date`
*/

CREATE OR REPLACE TABLE actors_history_scd (
    actor VARCHAR,
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH
(partitioning = ARRAY['current_year'])