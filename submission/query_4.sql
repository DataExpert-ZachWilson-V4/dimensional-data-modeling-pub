INSERT INTO actors_history_scd
WITH lagged AS (
SELECT
  actor,
  quality_class,
  is_active,
  LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) as quality_class_last_year,
  LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as is_active_last_year,
  current_year
FROM actors
WHERE current_year <= 2021
),
streaked AS (
SELECT 
  *,
  SUM(CASE WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1       
        ELSE 0 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
from lagged
)
SELECT 
  actor,
  quality_class,
  is_active,
  MIN(current_year) as start_date,
  MAX(current_year) as end_date,
  2021 AS current_year
FROM streaked
GROUP BY actor, quality_class, streak_identifier, is_active
