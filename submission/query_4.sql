INSERT INTO lsleena.actors_history_scd
WITH lagged AS (
  SELECT
    actor_id,
    COALESCE(is_active, false) AS is_active,
    LAG(COALESCE(is_active, false), 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
    COALESCE(quality_class, 'unknown') AS quality_class,
    LAG(COALESCE(quality_class, 'unknown'), 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year, -- Lagged columns to compare with the current year
    COALESCE(current_year, 0) AS current_year
  FROM lsleena.actors
  WHERE current_year <= (SELECT MAX(current_year) FROM lsleena.actors) -- Get the ,max(year) to include all available years
)
,streaked AS (
  SELECT
    *,
    SUM(IF(is_active != is_active_last_year OR quality_class != quality_class_last_year, 1, 0)) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier  -- Streak identifier to group consecutive years with the same values
  FROM lagged
)
SELECT
  actor_id,
  quality_class,
  is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  (SELECT MAX(current_year) FROM lsleena.actors) AS current_year
FROM streaked
GROUP BY
     actor_id,
     quality_class,
     is_active,
     streak_identifier -- Group by the streak identifier to get the start and end date of each change