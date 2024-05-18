-- Inserting data into the table "nancyatienno21998.actors_history_scd"
INSERT INTO nancyatienno21998.actors_history_scd

WITH
  -- CTE to compute lagged values for each actor
  lagged AS (
    SELECT
      actor,
      quality_class,
      current_year,
      -- Converting is_active to binary (1 for active, 0 for inactive)
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      -- Using LAG window function to get previous year's is_active status
      CASE
        WHEN LAG(is_active, 1) OVER (
            PARTITION BY actor
            ORDER BY current_year) THEN 1
        ELSE 0
      END AS is_active_last_year
    FROM
      nancyatienno21998.actors
    WHERE
      current_year <= 1921
  ),
  
  -- CTE to calculate streaks of activity for each actor
  streaked AS(
    SELECT
      *,
      -- Computing streak identifier by summing up changes in is_active status
      SUM(
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM lagged
  )

-- Selecting the final result set
SELECT
  actor,
  quality_class,
  -- Determining if the actor is currently active (MAX of is_active)
  MAX(is_active)=1 AS is_active,
  -- Finding the start date of the streak (MIN of current_year)
  MIN(current_year) AS start_date,
  -- Finding the end date of the streak (MAX of current_year)
  MAX(current_year) AS end_date,
  1921 as current_year
FROM
  streaked
GROUP BY
  actor,
  quality_class,
  streak_identifier
