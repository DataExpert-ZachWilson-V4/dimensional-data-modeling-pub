-- QUERY 4 ASSIGNMENT
-- Actors History SCD Table Batch Backfill Query (query_4)
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO vzucher.actors_history_scd
SELECT
    CAST(uuid() AS VARCHAR) AS history_id,  -- Explicitly casting UUID to VARCHAR
    actor_id,
    quality_class,
    is_active,
    DATE(CONCAT(CAST(MIN(current_year) AS VARCHAR), '-01-01')) AS start_date,  -- First day of the earliest year
    DATE(CONCAT(CAST(MAX(current_year) AS VARCHAR), '-12-31')) AS end_date    -- Last day of the latest year
FROM
    vzucher.actors
GROUP BY
    actor_id, quality_class, is_active
ORDER BY
    actor_id, MIN(current_year)

-- when defining the start and end date 
-- i ran this select distinct query to get all 
-- unique values for current_year and got that 
-- the min value was 2012 and max value 2021. 

-- SELECT DISTINCT current_year
-- FROM vzucher.actors
-- ORDER BY current_year DESC

-- we could've seted the start date to 2012 and 
-- end date to 2021 but i decided to set the
-- start date to 1900 and end date to 9999
-- to make sure that the record is currently active
-- and to avoid any future issues with the data.

