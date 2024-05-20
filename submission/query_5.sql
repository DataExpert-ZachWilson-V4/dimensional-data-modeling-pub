-- QUERY 5 ASSIGNMENT --

-- Actors History SCD Table Incremental Backfill Query (query_5)
-- Write an "incremental" query that can populate a single year's
-- worth of the actors_history_scd table by combining the previous year's SCD data 
-- with the new incoming data from the actors table for this year.

-- This query is designed to populate a single year's worth of data in the actors_history_scd table.
-- It combines the previous year's data with the new incoming data from the actors table for the current year.

INSERT INTO vzucher.actors_history_scd (history_id, actor_id, quality_class, is_active, start_date, end_date)
WITH lagged AS (
    SELECT *,
        LAG(quality_class) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS quality_class_last_year,
        LAG(is_active) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS is_active_last_year
    FROM vzucher.actors
),
changed AS (
    SELECT *,
        CASE
            WHEN quality_class <> quality_class_last_year OR is_active <> is_active_last_year THEN 1
            ELSE 0
        END AS changed
    FROM lagged
),
streak AS (
    SELECT *,
        SUM(changed) OVER(
            PARTITION BY actor_id
            ORDER BY current_year
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak
    FROM changed
),
aggregated AS (
    SELECT actor_id,
        actor,
        quality_class,
        is_active,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year,
        streak
    FROM streak
    GROUP BY actor_id, actor, quality_class, is_active, streak
)
SELECT 
    CAST(uuid() AS VARCHAR) AS history_id, -- Casting UUID to VARCHAR.
    actor_id,
    quality_class,
    is_active,
    DATE(CONCAT(CAST(start_year AS VARCHAR), '-01-01')) AS start_date, -- Direct conversion to DATE
    DATE(CONCAT(CAST(end_year AS VARCHAR), '-12-31')) AS end_date -- Direct conversion to DATE
FROM aggregated

--  The lagged CTE calculates the lagged values of quality_class and is_active for each actor_id.
-- This helps in identifying changes in these attributes compared to the previous year.
-- The streak CTE calculates the streak of changes for each actor_id.
-- It assigns a cumulative sum of the changed values, indicating the number of consecutive changes.
-- The changed CTE identifies whether there are any changes in quality_class or is_active compared to the previous year.
-- It assigns a value of 1 if there is a change, and 0 otherwise.
-- The aggregated CTE aggregates the data by actor_id, actor, quality_class, is_active, and streak.
-- It calculates the start_year and end_year based on the minimum and maximum current_year values.