INSERT INTO sagararora492.actors_history_scd
WITH lagging AS (
  SELECT
    actor_id,
    is_active,
    LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
    quality_class,
    LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
    current_year
  FROM sagararora492.actors
  WHERE current_year <= 1917
)
,streak AS (
SELECT
  *,
  SUM(IF(is_active != is_active_last_year OR quality_class != quality_class_last_year, 1, 0)) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_id
FROM lagging
)
SELECT
  actor_id,
  quality_class,
  is_active,
  MIN(current_year) AS start_year,
  MAX(current_year) AS end_year
FROM streak
GROUP BY 1,2,3, streak_id