INSERT INTO actors_history_scd
-- CTE to retrieve the previous year's is_active and quality_class for each actor
WITH lagged_activity AS (
  SELECT
    actor,
    is_active,
    -- Get the previous year's is_active status for each actor
    LAG(is_active, 1) OVER (
      PARTITION BY actor
      ORDER BY current_year
    ) AS is_active_last_year,
    quality_class,
    -- Get the previous year's quality_class for each actor
    LAG(quality_class, 1) OVER (
      PARTITION BY actor
      ORDER BY current_year
    ) AS quality_class_last_year,
    current_year
  FROM actors
  WHERE current_year <= 2021  -- Consider only data up to the year 2021
),
-- CTE to identify streaks of changes in is_active or quality_class
streak_change AS (
  SELECT
    actor,
    current_year,
    is_active,
    quality_class,
    -- Calculate a streak identifier based on changes in is_active or quality_class
    SUM(
      CASE
        WHEN is_active != is_active_last_year THEN 1
        WHEN quality_class != quality_class_last_year THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY actor ORDER BY current_year
    ) AS streak_identifier
  FROM lagged_activity
)
-- Select and group data to determine the start and end dates of each streak
SELECT
  actor,
  is_active,
  quality_class,
  MIN(current_year) AS start_date,  -- Get the start date of the streak
  MAX(current_year) AS end_date,  -- Get the end date of the streak
  2021 AS current_year  -- Set the current year to 2021
FROM streak_change
GROUP BY
  actor,
  streak_identifier,
  is_active,
  quality_class;

-- Testing the output table
-- SELECT *
-- FROM actors_history_scd
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year, start_date
