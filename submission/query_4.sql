INSERT INTO whiskersreneewe.actors_history_scd
WITH lagged AS (
SELECT 
  actor,
  actor_id,
  quality_class,
  is_active,
  LAG(is_active, 1) OVER (partition by actor, actor_id ORDER BY current_year) AS is_active_last_year,
  current_year
FROM whiskersreneewe.actors
),

streaked AS (
SELECT 
* ,
SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (partition by actor, actor_id ORDER BY current_year) AS streak_identifier
FROM lagged
)

SELECT 
  actor, actor_id, quality_class, 
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  current_year
FROM streaked
GROUP BY actor, actor_id, quality_class, streak_identifier, current_year