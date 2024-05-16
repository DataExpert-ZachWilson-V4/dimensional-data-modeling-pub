-- query_4
INSERT INTO actors_history_scd
WITH lagged AS (
  SELECT
    actor,
    quality_class,
    CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
    CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END AS is_active_last_year,
    current_year
    FROM actors
),
streaked AS (
  SELECT
    *,
    SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM lagged
)

SELECT
  actor,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2000 AS current_year
  FROM streaked
 GROUP BY actor, streak_identifier
