INSERT INTO actors_history_scd
WITH lagged AS (SELECT actor,
	  actor_id,
	  quality_class,
	  is_active,
	  current_year,
	  LAG(is_active,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS is_active_last_year,
	  LAG(quality_class,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS quality_class_last_year
	FROM actors
), streaked AS (SELECT *,
	  --A change either to is_active or quality_class designates a new streak
	  SUM(CASE WHEN is_active <> COALESCE(is_active_last_year,false) OR quality_class <> COALESCE(quality_class_last_year,'') THEN 1 ELSE 0 END) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS streak_identifier
	FROM lagged
), cy AS (SELECT MAX(current_year) as max_current_year
	FROM actors)
SELECT actor,
  actor_id,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  CAST(CAST(MIN(current_year) AS VARCHAR) || '-01-01' AS DATE) AS start_date,
  CAST(CAST(MAX(current_year) AS VARCHAR) || '-12-31' AS DATE) AS end_date,
  MAX(cy.max_current_year) AS current_year    --Dynamically set the most recent partition in the source dataset as the current_year
FROM streaked
	CROSS JOIN cy
GROUP BY actor,
  actor_id,
  streak_identifier
