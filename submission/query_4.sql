-- Actors History SCD Table Batch Backfill Query (query_4)
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO ttian45759.actors_history_scd 
-- Get all rows and calculate the previous year values)
-- We are partitioning by actor_id in case there happens to two actors with the same name.
WITH lagged AS (
  SELECT
    actor,
    actor_id,
    quality_class,
    CASE WHEN is_active then 1 ELSE 0 END AS is_active,
    CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) 
      THEN 1 ELSE 0 
    END AS is_active_last_year,
    LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as quality_class_last_year,
    current_year
  FROM
    ttian45759.actors
),

-- Generate a counter for when attributes quality_class and/or is_active changes.
streaked AS (
  SELECT 
    *,
    SUM(
      CASE 
        WHEN quality_class <> quality_class_last_year OR is_active <> is_active_last_year
      THEN 1 ELSE 0 END)
      OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
  FROM lagged
)

-- Get the changed rows by actor
SELECT 
  actor,
  actor_id,
  quality_class,
  CAST(is_active as BOOLEAN) as is_active, 
  MIN(current_year) as start_date,
  MAX(current_year) as end_date,
  current_year
FROM streaked
GROUP BY actor, actor_id, quality_class, is_active, current_year, streak_identifier