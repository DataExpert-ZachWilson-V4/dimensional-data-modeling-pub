-- This query inserts actor data into the vpsjul8468082.actors_history_scd table.
-- It uses a Type 2 Slowly Changing Dimension (SCD) approach to track changes in actors' quality_class and active status over time.
-- The query identifies changes in status, computes streaks of consecutive years with the same status, and records the start and end dates for each streak.


INSERT INTO vpsjul8468082.actors_history_scd
-- Get is_active and quality_class from the previous year using LAG for a given actor
WITH lagged AS (
  SELECT 
    actor, 
    CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
    CASE WHEN LAG(is_active) OVER(PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END AS is_active_last_year,
    quality_class,
    LAG(quality_class) OVER(PARTITION BY actor ORDER BY current_year) AS quality_class_last_year, 
    current_year
  FROM richiesingh.actors 
  WHERE current_year <= 2021
),
-- Find the streak to see when the status changed
streaked AS (
  SELECT *,
    SUM(CASE WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year
             THEN 1 ELSE 0 END) OVER(PARTITION BY actor ORDER BY current_year) AS streak_identifier
  FROM lagged
)
-- Find the start and end dates for each streak
SELECT 
  actor,
  quality_class,
  MAX(is_active) = 1 AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year
FROM streaked
GROUP BY actor, quality_class, streak_identifier
