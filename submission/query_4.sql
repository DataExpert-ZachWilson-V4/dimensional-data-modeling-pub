INSERT INTO tdueck66966.actors_history_scd
WITH lagged AS ( --CTE to capture current and last year's film activity and current and last year's quality class
  SELECT
    actor_id,
    actor,
    CASE WHEN is_active THEN 1 ELSE 0 END as is_active,
    CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) THEN 1 ELSE 0 END as is_active_last_year,
    quality_class,
    LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as qc_last_year, --added this column to also identify streaks for changed quality class
    current_year
  FROM tdueck66966.actors
  WHERE current_year <= 2010
),
streaked AS ( --CTE to evaluate film streaks by changes to activity and quality class
  SELECT
    *,
    CASE WHEN is_active <> is_active_last_year OR quality_class <> qc_last_year THEN 1 ELSE 0 END as streak, --checked for changes in either active year or quality class
    SUM(CASE WHEN is_active <> is_active_last_year OR quality_class <> qc_last_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year) as streak_identifier
  FROM lagged
)

SELECT
  actor_id,
  actor,
  quality_class,
  CAST(MAX(is_active) AS BOOLEAN) as is_active, --populates the true/false value for the current year's activity status
  MIN(current_year) as start_date, --populates the first year for the given streak of activity and quality class
  MAX(current_year) as end_date, --populates the last year for the given streak of activity and quality class
  2010 as current_year
FROM streaked
GROUP BY
  actor_id,
  actor,
  quality_class,
  streak_identifier
