INSERT INTO  
  jrsarrat.actors_history_scd   --12572 rows inserted for 2021.  12195 for 2020.  11040 for 2019.  8638 for 2018.
WITH
  lagged AS (
    SELECT
      actor,
      quality_class,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) THEN 1
        ELSE 0
      END AS is_active_last_year,
      current_year
    FROM
      jrsarrat.actors
        WHERE current_year <= 2018
  
  ),
  tally_history AS (
    SELECT
      *,
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
      ) AS actor_identifier
    FROM
      lagged
  )
SELECT
  actor,
  quality_class,
  MAX(is_active) = 1 AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2018 AS current_year
FROM
  tally_history
GROUP BY
  actor,
  quality_class,
  actor_identifier

--------------------------------------------------------------------------

INSERT INTO  
  jrsarrat.actors_history_scd 
WITH last_year_scd AS (
  SELECT 
    * 
  FROM jrsarrat.actors_history_scd
    WHERE current_year = 2020
),
current_year_scd AS (
  SELECT 
    * 
  FROM jrsarrat.actors
    WHERE current_year = 2021
)
 
 SELECT * FROM last_year_scd 
