INSERT INTO
deeptianievarghese22866.actors_history_scd
WITH
  lagged AS (   --tracking if an actor is active in current and previous years
    SELECT
      actor,
      quality_class,
      current_year,
       is_active,  --statement to determine actor's active status in current year
     LAG(is_active, 1) OVER (     --statement to determine actor's active status in previous year
          PARTITION BY
            actor
          ORDER BY
            current_year
        )  AS is_active_last_year,
       LAG(quality_class, 1) OVER (    --statement to determine actor's quality class in previous year
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) AS quality_class_last_year
    FROM
      deeptianievarghese22866.actors
    WHERE
      current_year <= 2021   --read all historic records upto 2021
  ),
  streaked AS (   --tracking state change for each actor - change from active-->inactive, inactive-->active
    SELECT
      *,
      SUM(            --summing up number of state changes for each actor ordered by year- change from active-->inactive, inactive-->active
        CASE
          WHEN (
    quality_class <> quality_class_last_year
    OR is_active <> is_active_last_year
  )
    THEN 1
  WHEN (
    quality_class = quality_class_last_year
    AND is_active = is_active_last_year
  )
    THEN 0 END)
  OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM
      lagged
  )
SELECT
  actor,
  quality_class,
  is_active,
  MIN(current_year) AS start_date, --start and end year for an actor's active streak
  MAX(current_year) AS end_date,
2021 as current_year
FROM
  streaked
GROUP BY
  actor,
  quality_class,
  is_active,
streak_identifier
