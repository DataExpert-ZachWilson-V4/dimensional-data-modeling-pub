-- Below is a backfill query that populates the entire actors_history_scd table in a single query till the year 2020
INSERT INTO RaviT.actors_history_scd
WITH
  -- Subquery to fetch the previous year's is_active and quality_class values using the LAG function
  lagged AS (
    SELECT
      actor_id,
      is_active,
      -- Below column is using the LAG function to get the previous year's is_active value
      LAG(is_active, 1) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS is_active_last_year,
      -- Below column is using the LAG function to get the previous year's quality_class value
      quality_class,		
      LAG(quality_class, 1) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS quality_class_last_year,	
      current_year
    FROM
      RaviT.actors
    WHERE
      current_year <= 2020
  ),
  -- Subquery to create identifiers for changes in is_active and quality_class to track changes over time
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year 
      ) AS streak_is_active_identifier,
      SUM(
        CASE
          WHEN quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year 
      ) AS streak_quality_class_identifier
    FROM
      lagged
  )
-- Main query to insert into actors_history_scd table
SELECT
  actor_id,
  MAX(quality_class) AS quality_class, -- Get the latest quality_class in the streak
  MAX(is_active) AS is_active, -- Get the latest is_active status in the streak
  MIN(current_year) AS start_date, -- Start date of the streak
  MAX(current_year) AS end_date, -- End date of the streak
  2020 AS current_year -- Fixed current year for this backfill
FROM
  streaked
GROUP BY
  actor_id,
  streak_is_active_identifier,
  streak_quality_class_identifier
