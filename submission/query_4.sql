INSERT INTO jsgomez14.actors_history_scd
WITH lagged AS (
  SELECT
    actor_id,
    is_active,
    LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
    quality_class,
    LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
    current_year
    -- Lagged columns to compare with the current year.
  FROM jsgomez14.actors
  WHERE current_year <= 1917
  -- Filter the data to the specific years you want to backfill.
)
,streaked AS (
SELECT
  *,
  -- Streak identifier to group consecutive years with the same values.
  SUM(IF(is_active != is_active_last_year OR quality_class != quality_class_last_year, 1, 0)) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
FROM lagged
)
SELECT
  actor_id,
  quality_class,
  is_active,
  MIN(current_year) AS start_year,
  MAX(current_year) AS end_year
  -- Group by the streak identifier to get the start and end year of each change.
FROM streaked
GROUP BY 1,2,3, streak_identifier