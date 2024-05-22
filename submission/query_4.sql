INSERT INTO
  actors_history_scd 
WITH
  lagged AS (
    SELECT
      actor,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      -- Use LAG to get the is_active status of the previous year
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY actor
          ORDER BY current_year
        ) THEN 1
        ELSE 0
      END AS is_active_last_year,
      quality_class,
      LAG(quality_class, 1) OVER (
        PARTITION BY actor
        ORDER BY current_year
      ) AS last_year_quality_class,
      current_year
    FROM
      actors_history_scd
    WHERE
      -- Filter the records to include seasons up to and including 2021
      current_year <= 2021
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          -- If is_active or quality_class varies, the streak identifier should be increased.
          WHEN is_active <> is_active_last_year OR quality_class <> last_year_quality_class THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY actor
        ORDER BY current_year
      ) AS streak_identifier
    FROM
      lagged
  ),
  periods AS (
    SELECT
      actor,
      quality_class,
      MAX(is_active) = 1 AS is_active,
      MIN(current_year) AS start_year,
      MAX(current_year) AS end_year,
      -- Set the current year as 2021 for all records in this backfill
      2021 AS current_year
    FROM
      streaked
    GROUP BY
      actor,
      streak_identifier,
      quality_class
  )
SELECT
  actor,
  quality_class,
  is_active,
  start_year AS start_date,
  end_year AS end_date,
  current_year
FROM
  periods
ORDER BY
  actor,
  start_date
