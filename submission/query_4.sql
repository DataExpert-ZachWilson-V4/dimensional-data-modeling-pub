-- a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
-- Our data spans from 1914 to 2021
-- Loaded a sample from 1914 to 1923 to test
INSERT INTO siawayforward.actors_history_scd
WITH actor_history AS (
  SELECT
      actor_id,
      actor,
      current_year,
      quality_class,
      is_active,
      -- capture last year of current quality status
      COALESCE(LAG(quality_class, 1) OVER(actor_win),'') = COALESCE(quality_class, '') 
      AND 
      -- capture last year of current active status
      COALESCE(LAG(is_active, 1) OVER(actor_win), FALSE) = is_active AS matches_last_year
  FROM siawayforward.actors
  WHERE current_year <= 1923
  -- using Milton Berle, Lillian Gish, Charlie Chaplin for testing
  -- they have good use cases for changing dims
  -- AND actor_id IN ('nm0000926', 'nm0001273', 'nm0000122')
  GROUP BY 1, 2, 3, 4, 5
  -- window to define slowly changing dimension being tracked
  WINDOW actor_win AS (PARTITION BY actor_id ORDER BY current_year)

), actor_scd_streak AS (
  SELECT
    *,
    -- get how the quality_class and is_active change FLAG year over year for each actor
    SUM(IF(matches_last_year, 1, 0)) OVER(PARTITION BY actor_id, quality_class, is_active, matches_last_year, current_year ORDER BY actor_id, matches_last_year, quality_class, is_active, current_year) AS streak_id
  FROM actor_history
  ORDER BY current_year
  
), actor_non_consecutive_streak AS (

  SELECT actor_id,
      actor,
      current_year,
      quality_class,
      is_active,
      matches_last_year,
      streak_id,
      -- use the streak_id flag to accumulate non consecutive streaks 
      -- e.g. if the same thing happened from 2020-2021 and 2022-2025, that's 2 and 4, two different streak counts
      CASE 
        -- there is a gap, the last year observed is more than a year ago, restart the streak
        WHEN 
       current_year - 
       LAG(current_year, 1) 
        OVER(
            PARTITION BY actor_id, quality_class, is_active 
            ORDER BY actor_id, quality_class, is_active, current_year
        ) > 1 THEN 0
       -- the previous occurence was a new streak
       WHEN LAG(streak_id, 1) OVER(PARTITION BY actor_id ORDER BY current_year) = 0 THEN streak_id
       ELSE
       SUM(streak_id) 
        OVER(
            PARTITION BY actor_id, quality_class, is_active, matches_last_year 
            ORDER BY actor_id, quality_class, is_active, current_year
        ) 
      END AS cumm_streak_id,
      -- flag to track when a strak restarted so we can use it to create scd time dimension years
      CASE WHEN 
       LAG(current_year, 1) 
        OVER(
            PARTITION BY actor_id, quality_class, is_active 
            ORDER BY actor_id, quality_class, is_active, current_year
        ) IS NOT NULL
       AND 
       current_year - 
       LAG(current_year, 1)
        OVER(
            PARTITION BY actor_id, quality_class, is_active 
            ORDER BY actor_id, quality_class, is_active, current_year
        ) > 1 THEN TRUE
       ELSE FALSE END AS streak_restarts
  FROM actor_scd_streak
  
), scd_start AS (
  SELECT
    *,
    CASE 
      -- when this is a new streak for statuses
      WHEN NOT matches_last_year THEN current_year
      -- when things have not changed since last year
      WHEN NOT streak_restarts THEN current_year - cumm_streak_id
      -- when we've seen the scd before but there was a gap
      WHEN streak_restarts THEN current_year
      ELSE NULL END AS start_date,
    
    CASE 
      -- when things have changed in the next year for a specific actor
      WHEN LEAD(cumm_streak_id, 1) OVER(scd) = 0 THEN current_year
      -- no change for actor
      ELSE MAX(current_year) OVER(scd) END
     AS end_date
  FROM actor_non_consecutive_streak
  WINDOW scd AS (PARTITION BY actor_id, quality_class, is_active ORDER BY current_year)
  
)
SELECT 
  actor_id,
  actor,
  quality_class,
  is_active,
  start_date,
  MAX(end_date) AS end_date,
  1923 AS current_year
FROM scd_start
GROUP BY 1, 2, 3, 4, 5, 7
ORDER BY 1, 5
