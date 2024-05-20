/* A DDL "backfill" query that can populate the entire actors_history_scd table in a single query */

INSERT INTO supreethkabbin.actors_history_scd
-- Track is_active and quality_class for an actor from the previous year
WITH lagged AS (
  SELECT
    actor, 
    actor_id, 
    quality_class,
    LAG(quality_class, 1) OVER(PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
    is_active, 
    LAG(is_active, 1) OVER(PARTITION BY actor, actor_id ORDER BY current_year) is_active_last_year,
    current_year
  FROM 
    supreethkabbin.actors
  WHERE current_year <= 2020
), 
-- Track if change in is_active and quality class fields
active_class_change AS (
  SELECT
    actor,
    actor_id, 
    quality_class,
    is_active,
    quality_class_last_year,
    is_active_last_year, 
    SUM(CASE 
          WHEN is_active <> is_active_last_year
            AND quality_class <> quality_class_last_year THEN 1 
          ELSE 0
        END) OVER(PARTITION BY actor, actor_id ORDER BY current_year) AS change_identifier, 
    current_year
  FROM
    lagged
)
SELECT 
  actor, 
  actor_id, 
  quality_class,
  is_active, 
  MIN(current_year) AS start_date, 
  MAX(current_year) AS end_date, 
  2020 as current_year
FROM 
  active_class_change
GROUP BY 
  actor, 
  actor_id, 
  quality_class, 
  is_active, 
  change_identifier