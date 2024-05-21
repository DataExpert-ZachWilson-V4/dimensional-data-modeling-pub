INSERT INTO denzelbrown.actors_history_scd
WITH lagged as (
SELECT 
  actor,
  CASE WHEN is_active then 1 ELSE 0 END as is_active,
  CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END as is_active_last_year,
  current_year,
  quality_class
FROM denzelbrown.actors ),

streaked as (
SELECT *, SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ElSE 0 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
FROM lagged
)

SELECT actor,
quality_class,
MAX(is_active)=1 as is_active,
MIN(current_year) as start_date,
MAX(current_year) as end_date,
1923 AS current_year
FROM streaked
GROUP BY actor, streak_identifier, quality_class
