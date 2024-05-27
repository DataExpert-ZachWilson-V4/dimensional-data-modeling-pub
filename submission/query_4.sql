-- Load actors history by tracking
-- changes in quality_class and is_active
-- fields. Make sure the start_date
-- and end_date are correctly set

INSERT INTO iliamokhtarian.actors_history_scd
WITH lagged AS (
  -- Fetch the previous year's quality_class and is_active for each actor
  SELECT
    *,
    LAG(quality_class, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS prev_quality_class,
    LAG(is_active, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS prev_is_active
  FROM
    iliamokhtarian.actors
  WHERE
    current_year <= 2021
),
streaked AS (
  -- Identify streaks where quality_class or is_active changes
  SELECT
    *,
    SUM(
      CASE
        WHEN quality_class = prev_quality_class
        AND is_active = prev_is_active THEN 0
        ELSE 1
      END
    ) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS change_identifier
  FROM
    lagged
)
SELECT
  actor,
  actor_id,
  quality_class,
  is_active,
  -- Set the start date to January 1st of the first year in the streak
  MIN(DATE_PARSE(
    CAST(current_year AS VARCHAR) || '-01-01',
    '%Y-%m-%d'
  )) AS start_date,
  -- Set the end date to December 31st of the last year in the streak, unless it is the current year
  COALESCE(
    MAX(DATE_PARSE(
      CAST(current_year AS VARCHAR) || '-12-31',
      '%Y-%m-%d'
    )),
    DATE '9999-12-31' -- Use a far future date to indicate that the record is currently active
  ) AS end_date,
  2021 AS current_year
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  quality_class,
  is_active,
  change_identifier
