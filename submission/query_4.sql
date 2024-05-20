-- Insert data into the halloweex.actors_history_scd table
INSERT INTO  halloweex.actors_history_scd
WITH
  -- Step 1: Retrieve the data and calculate previous year's is_active status using LAG
  lagged AS (
    SELECT
      actor,
      quality_class,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,  -- Convert is_active to 1 or 0
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY actor
          ORDER BY current_year
        ) THEN 1
        ELSE 0
      END AS is_active_last_year,  -- Calculate the is_active status of the previous year
      current_year
    FROM
      halloweex.actors
    WHERE current_year <= 2021  -- Process data up to the year 2021
  ),

  -- Step 2: Calculate streaks where is_active status changes
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY actor
        ORDER BY current_year
      ) AS streak_identifier  -- Calculate a streak identifier based on changes in is_active status
    FROM
      lagged
  )

-- Step 3: Insert the new records into the actors_history_scd table
SELECT 
  actor,
  quality_class,
  MIN(current_year) AS start_date,  -- Start date of the streak
  MAX(current_year) AS end_date,  -- End date of the streak
  2021 AS current_year,
  MAX(current_year) = 2021 AS is_current,  -- Check if the record is current
  MAX(is_active) = 1 AS is_active  -- Check if the actor is active
FROM
  streaked
GROUP BY
  actor,
  quality_class,
  streak_identifier  -- Group by actor, quality_class, and streak identifier
ORDER BY
  actor,
  start_date;  -- Order by actor and start date
