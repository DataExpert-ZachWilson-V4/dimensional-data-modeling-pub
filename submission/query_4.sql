INSERT INTO martinaandrulli.actors_history_scd
WITH 
    --Table to get if the actor was active or not the previous year
    active_info_cte AS ( 
        SELECT
            actor,
            quality_class,
            actor_id,
            is_active,
            -- Window function to get if an entry for the actor exists also in the previous year. If so, it returns 1 
            LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year, 
            current_year
        FROM
            martinaandrulli.actors
    ),
    streak_cte AS (
        SELECT *,
            -- By looking at each actor, an index of his "behaviour" is established. 
            -- For each year, it checks the previous ones and compute the following: if there is a change in the behaviour of the actor from one year to the other, his ID increase. If not, it remains stable.
            SUM( 
                CASE
                    WHEN is_active <> is_active_last_year THEN 1
                ELSE 0
                END
                ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_id
        FROM active_info_cte
    )
SELECT
  actor_id,
  quality_class,
  -- is_actibe is true or false based on the value of it has in the timerange
  MAX(is_active) AS is_active,
  -- start_date is the lower bound of the current year timerange of the group identified
  MIN(current_year) AS start_date, 
  -- end_date is the upper bound of the current year timerange of the group identified
  MAX(current_year) AS end_date,
  1919 AS current_year
FROM
  streak_cte
GROUP BY
  actor_id,
  streak_id,
  -- By grouping by also the quality_class, we ensure that a new entry is added even if the streak_id remains the same but the quality_class has changed (the actor is still active in the new year but he has increased his quality class)
  quality_class 