INSERT INTO nancycast01.actors_history_scd
WITH
  lagged AS (
    SELECT
      actor,
      quality_class,
      is_active,
      LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS is_active_last_year,
      LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS quality_class_last_year,
      current_year
    FROM
      nancycast01.actors
    WHERE
      current_year <= 2021
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END) OVER ( PARTITION BY actor ORDER BY current_year
      ) AS streak_identifier
    FROM
      lagged
  )
SELECT
  actor,
  quality_class,
  is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year

FROM
  streaked
 
GROUP BY
  actor,
  quality_class,
  is_active,
  streak_identifier