WITH lagged AS (
  -- This CTE (Common Table Expression) calculates additional columns based on the sravan.actors table
  SELECT
    actor, -- The name of the actor
    quality_class, -- The quality class of the actor
    is_active, -- The current active status of the actor (boolean)
    CASE
      -- This CASE expression checks if the actor's active status changed compared to the previous year
      -- It uses the LAG function to access the previous year's value for the same actor
      WHEN LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) <> is_active THEN 1
      ELSE 0
    END AS is_active_changed,
    CASE
      -- This CASE expression checks if the actor's quality class changed compared to the previous year
      -- It uses the LAG function to access the previous year's value for the same actor
      WHEN LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) <> quality_class THEN 1
      ELSE 0
    END AS quality_class_changed,
    current_year -- The current year
  FROM sravan.actors
)
, streaked AS (
  -- This CTE builds upon the lagged CTE and calculates a streak_identifier
  SELECT
    *,  -- Select all columns from the lagged CTE
    SUM(
      CASE
        -- This CASE expression checks if either is_active_changed or quality_class_changed is 1
        -- If either of them is 1, it means the actor's status or quality class changed, so we increment the streak_identifier
        WHEN is_active_changed = 1 OR quality_class_changed = 1 THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    -- The SUM window function calculates a running sum of the CASE expression
    -- It partitions the data by actor and orders it by current_year
    -- This way, the streak_identifier increments whenever the actor's status or quality class changes
  FROM lagged
)
-- Improved final SELECT with comments
SELECT
  actor, -- The name of the actor
  quality_class, -- The quality class of the actor
  is_active, -- The current active status of the actor (no need for MAX() since we want the current value)
  MIN(current_year) OVER (PARTITION BY actor, streak_identifier) AS start_date,
  -- The MIN window function calculates the minimum current_year for each actor and streak_identifier combination
  -- This gives us the start year of each streak
  MAX(current_year) OVER (PARTITION BY actor, streak_identifier) AS end_date,
  -- The MAX window function calculates the maximum current_year for each actor and streak_identifier combination
  -- This gives us the end year of each streak
  2021 AS current_year -- A constant value representing the current year (adjust if necessary)
FROM streaked
