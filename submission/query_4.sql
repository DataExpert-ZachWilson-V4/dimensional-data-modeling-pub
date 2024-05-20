INSERT INTO actors_history_scd
WITH previous_year_status AS (
  SELECT
    actor,
    is_active,
    LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS is_active_last_year,
    quality_class,
    LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS quality_class_last_year,
    current_year
  FROM actors
  WHERE current_year <= (SELECT MAX(current_year) FROM actors)
),
status_change_identifier AS (
  SELECT
    *,
    SUM(IF(is_active != is_active_last_year OR quality_class != quality_class_last_year, 1, 0)) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
  FROM previous_year_status
)
SELECT
  actor,
  quality_class,
  is_active,
  MIN(current_year) AS start_year,
  MAX(current_year) AS end_year,
  MAX(current_year) AS current_year
FROM status_change_identifier
GROUP BY 1,2,3, streak_identifier
