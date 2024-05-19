INSERT INTO actors_history_scd
WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      LAG(quality_class, 1) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) quality_class_last_year,
      is_active,
      LAG(is_active, 1) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) is_active_last_year,
      current_year
    FROM
      actors
    WHERE current_year <= 2021 -- Ensure we only include records up to the current year
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year
          OR quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM
      lagged
  )
SELECT
  actor,
  actor_id,
  quality_class,
  is_active,
  min(current_year) start_date,
  max(current_year) end_date,
  2021 AS current_year -- Update the current year to the correct value
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  quality_class,
  is_active,
  streak_identifier