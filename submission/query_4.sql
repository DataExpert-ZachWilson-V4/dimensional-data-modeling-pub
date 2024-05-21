INSERT INTO jessiii.actors_history_scd 
-- 'lagged' CTE: Prepares data by fetching previous year's 'is_active' and 'quality_class' for each actor. 
-- It uses the SQL LAG function to access data from the previous row partitioned by actor_id and ordered by current_year. 
-- For the first year of data (I arbitrarily picked 1993), the previous year's data points are unknown and thus set to NULL.
WITH lagged AS (
    SELECT
        actor,
        actor_id,
        is_active,
        quality_class,
        current_year,
        -- Check if current_year is the first year; if so, previous state data is unavailable and set to NULL. Else, fetch the previous year's is_active state.
        CASE
            WHEN current_year = 1993
                THEN NULL ELSE
                coalesce(
                    lag(is_active, 1)
                        OVER (PARTITION BY actor_id ORDER BY current_year),
                    FALSE
                )
        END AS is_active_last_year,
        -- Fetch previous year's quality_class, set to NULL if current_year is the first year.
        CASE
            WHEN current_year = 1993
                THEN NULL ELSE
                lag(quality_class)
                    OVER (PARTITION BY actor_id ORDER BY current_year)
        END AS quality_class_last_year
    FROM jessiii.actors
    WHERE current_year <= 2001 -- Consider data up to the year 2001 for backfilling.
),
-- 'streaked' CTE: Calculates the continuity or changes ('streaks') in 'is_active' and 'quality_class' over the years.
streaked AS (
SELECT
    *,
    -- Sum increments each time there is a change in the 'is_active' state compared to the last year, or if it is the first year.
    SUM(CASE
        WHEN is_active <> is_active_last_year THEN 1
        WHEN is_active_last_year IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year)
        AS is_active_streak,
     -- Sum increments each time there is a change in the 'quality_class' compared to the last year, or if it is the first year.   
    SUM(CASE
        WHEN quality_class <> quality_class_last_year THEN 1
        WHEN quality_class_last_year IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year)
        AS quality_class_streak
FROM lagged
)
-- Final insertion from the 'streaked' CTE into the history table.
SELECT
  actor,
  actor_id,
  -- Capture the most recent 'quality_class' from the grouped period.
  MAX(quality_class) as quality_class,
  -- Determine if the actor was active at least once in the grouped period.
  MAX(is_active) as is_active,
  -- Record the start year of the current streak.
  MIN(current_year) as start_date,
  -- Record the end year of the current streak.
  MAX(current_year) as end_date,
  -- The backfill is considered for the year 2001 as the base year for this history.
  2001 as current_year
FROM streaked
GROUP BY actor, actor_id, is_active_streak, quality_class_streak
-- Group by actor and the identified streaks to segment the history accurately.

