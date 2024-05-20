-- Loading actory history by monitoring changes in 'quality_class' and 'is_active'
-- There will be a new entry when one of 'quality_class' or 'is_active' changes


-- Creating a CTE to check
-- Quality class of actor last year AND
-- Whether the actor was active last year
INSERT INTO pratzo.actors_history_scd
WITH lagged AS (
SELECT
  actor,
  quality_class,
  CASE
    WHEN is_active THEN 1
    ELSE 0
  END AS is_active,
  CASE WHEN LAG(is_active, 1) OVER (
    PARTITION BY
      actor
    ORDER BY
      current_year
  ) THEN 1 ELSE 0
  END AS is_active_last_year,
  LAG(quality_class, 1) OVER(
        PARTITION BY
          actor_id
        ORDER BY
          current_year
  ) AS quality_class_last_year,
  current_year
FROM
  pratzo.actors
  WHERE current_year <= 1978
  
),

-- Creating a CTE to track streaks of the same quality class and active status for an actor
-- The streak breaks when one of 'quality_class' or 'is_active' changes
streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year 
           OR  quality_class <> quality_class_last_year
           THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
      ) AS streak_identifier
      
    FROM
      lagged
  )

select 
actor, 
quality_class,
is_active = 1,  -- Adding this condition as is_active is a Boolean but got converted to int in lagged
MIN(current_year) as start_year, 
MAX(current_year) as end_year,
1978 as current_year
from 
    streaked
group by 
actor, 
streak_identifier, 
quality_class,
is_active
