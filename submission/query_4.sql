--query_4
/* backfill query */


INSERT INTO hdamerla.actors_history_scd
WITH LAGGED AS (
SELECT 
  actor,
  actor_id,
  quality_class,
  CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
  CASE WHEN  LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END AS is_active_last_year, --actor active previous year
  LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
  current_year 
FROM hdamerla.actors
),

  streaked AS (
SELECT
  *,
  SUM(
    CASE
      WHEN is_active <> is_active_last_year or quality_class <> quality_class_last_year
        THEN 1
      ELSE 0
    END
  ) OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier --changes in actor values from one year to the next
FROM LAGGED
)

SELECT
  actor,
  actor_id,
  quality_class,
  MAX(is_active) = 1 as is_active, -- in lagged CTE boolean is converted to INT like in the labs/lecture
  MIN(current_year) as start_date,
  MAX(current_year) as end_date,
  2021 as current_year
FROM streaked
GROUP BY actor, actor_id, streak_identifier, quality_class, is_active
