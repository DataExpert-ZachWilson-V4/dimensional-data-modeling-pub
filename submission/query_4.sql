INSERT INTO tejalscr.actors_history_scd
WITH LAGGED AS (SELECT
  actor,
  actor_id,
  quality_class,
  LAG(quality_class,1) OVER(PARTITION BY actor_id ORDER BY current_year) as last_quality_class,
  CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
  CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) THEN 1 ELSE 0 END AS is_active_last_year,
  current_year
  FROM
  tejalscr.actors 
  where current_year <= 1924 ) 
,ACTIVE_QUALITY_CLASS_STREAKED AS (
 select *, 
 SUM(case when is_active<>is_active_last_year and quality_class<>last_quality_class then 1 else 0 end) over(partition by actor_id order by current_year) as streak_identifier
from LAGGED  )
select 
actor,
actor_id,
MAX(quality_class) as quality_class,
MAX(is_active) = 1 as is_active,
min(Current_year) as start_date,
max(current_year) as end_date,
1924 as current_year
from ACTIVE_QUALITY_CLASS_STREAKED
group by actor, actor_id, streak_identifier


