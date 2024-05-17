-- This SQL query can backfill the entire actors_history_scd table in a single query.

INSERT INTO anjanashivangi.actors_history_scd 
WITH lagged AS (
  SELECT
    actor,
    actor_id,
    films,
    quality_class,
    CASE
      WHEN is_active THEN 1
      ELSE 0
    END AS is_active,
    CASE
      WHEN LAG(is_active, 1) OVER (
        PARTITION BY actor_id
        ORDER BY
          current_year
      ) THEN 1
      ELSE 0
    END AS is_active_last_year,
    LAG(quality_class, 1) OVER (
      PARTITION BY actor_id
      ORDER BY
        current_year
    ) AS qc_last_year, -- Use LAG() function to get last year's quality_class value
    current_year
  FROM
    anjanashivangi.actors
  WHERE
    current_year <= 1930 -- Hardcoded to the year upto which we want to backfill
),
streaked AS (
  SELECT
    *,
    SUM(
      CASE
        WHEN is_active <> is_active_last_year
        or quality_class <> qc_last_year THEN 1 -- Updates Streak_identifier, if there is change in either is_active or quality_class
        ELSE 0
      END
    ) OVER (
      PARTITION BY actor_id
      ORDER BY
        current_year
    ) AS streak_identifier
  FROM
    lagged
)
SELECT
  actor,
  actor_id,
  MAX(quality_class) as quality_class,
  MAX(is_active) = 1 AS is_active,
  MIN(current_year) AS start_year,
  MAX(current_year) AS end_year,
  1930 AS current_year -- Hardcoded current year
FROM
  streaked
GROUP BY
  actor_id,
  actor,
  streak_identifier