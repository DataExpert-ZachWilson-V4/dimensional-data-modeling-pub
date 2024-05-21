INSERT INTO ebrunt.actors_history_scd WITH lagged AS (
  SELECT 
    actor_id, 
    quality_class, 
    is_active, 
    current_year, 
    LAG(is_active, 1) OVER (
      PARTITION BY actor_id 
      ORDER BY 
        current_year
    ) as active_last_year, 
    LAG(quality_class, 1) OVER (
      PARTITION BY actor_id 
      ORDER BY 
        current_year
    ) as quality_class_last_year 
  FROM 
    ebrunt.actors
), 
streaked AS (
  SELECT 
    *, 
    SUM(
      CASE WHEN is_active <> active_last_year 
      OR quality_class <> quality_class_last_year THEN 1 ELSE 0 END
    ) OVER (
      PARTITION BY actor_id 
      ORDER BY 
        current_year
    ) AS streak 
  FROM 
    lagged
) 
SELECT 
  actor_id, 
  MAX(quality_class) as quality_class, 
  MAX(is_active) as is_active, 
  MIN(current_year) as start_date, 
  MAX(current_year) as end_date, 
  1960 as current_year 
FROM 
  streaked 
GROUP BY 
  actor_id, 
  streak
