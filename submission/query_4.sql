INSERT INTO
  actors_history_scd 

WITH
  lagged AS (
    SELECT
      actor,
      is_active,
      LAG(is_active, 1) OVER (
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) AS is_active_last_season,
        
      quality_class,
      LAG(quality_class, 1) OVER (
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) AS quality_class_last_season,
      current_year
    FROM
      actors
    WHERE
      current_year <= 2019
  ),

  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_season OR quality_class <> quality_class_last_season THEN 1
          ELSE 0
        END
      ) OVER (
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
  MIN(current_year) AS start_season,
  MAX(current_year) AS end_season,
  2020 AS current_season
FROM
  streaked
GROUP BY
  actor,
  streak_identifier,
  is_active,
  quality_class
ORDER BY 
actor,
streak_identifier