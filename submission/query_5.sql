/* An "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD data with the new incoming data from the actors table for this year */

INSERT INTO actors_history_scd
-- Retrieve last years actors SCD data
WITH last_year AS (
  SELECT 
    *
  FROM 
    actors_history_scd
  WHERE
    current_year = 2020
), 
-- Retrieve current years actors data 
this_year AS (
  SELECT
    *
  FROM 
    actors
  WHERE
    current_year = 2021
),
-- CTE to track combined records and changes in is_active and quality_class
combined AS (
  SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    COALESCE(ly.start_date, ty.current_year) AS start_date,
    COALESCE(ly.end_date, ty.current_year) AS end_date,
    CASE 
      WHEN ly.is_active <> ty.is_active AND ly.quality_class <> ty.quality_class THEN 1
      WHEN ly.is_active = ty.is_active AND ly.quality_class = ty.quality_class THEN 0
    END AS did_change,
    ly.quality_class AS last_year_quality_class,
    ty.quality_class AS this_year_quality_class,
    ly.is_active AS is_active_last_year,
    ty.is_active AS is_active_this_year,
    2021 AS current_year
  FROM
    last_year ly
  FULL OUTER JOIN
    this_year ty
      ON ly.actor_id = ty.actor_id
      AND ly.end_date + 1 = ty.current_year
), 
-- CTE to act on changes to is_active and quality_class
changes AS (
  SELECT 
    actor, 
    actor_id,
    current_year, 
    CASE 
      WHEN did_change = 0 THEN 
        ARRAY[
          CAST(ROW(
            is_active_last_year, 
            last_year_quality_class, 
            start_date, 
            end_date + 1
          ) AS ROW(
            is_active BOOLEAN, 
            quality_class VARCHAR, 
            start_date INTEGER, 
            end_date INTEGER
            ))
        ]
      WHEN did_change = 1 THEN
        ARRAY[
          CAST(ROW(
            is_active_last_year, 
            last_year_quality_class, 
            start_date, 
            end_date
          ) AS ROW(
            is_active BOOLEAN, 
            quality_class VARCHAR, 
            start_date INTEGER, 
            end_date INTEGER
          )),
          CAST(ROW(
            is_active_this_year, 
            this_year_quality_class, 
            start_date, 
            end_date
          ) AS ROW(
          is_active BOOLEAN, 
          quality_class VARCHAR, 
          start_date INTEGER, 
          end_date INTEGER
          ))
        ]
      WHEN did_change IS NULL THEN 
        ARRAY[
          CAST(ROW(
            COALESCE(is_active_last_year, is_active_this_year), 
            COALESCE(last_year_quality_class, this_year_quality_class), 
            start_date, 
            end_date
          ) AS ROW(
            is_active BOOLEAN, 
            quality_class VARCHAR, 
            start_date INTEGER, 
            end_date INTEGER
          ))
        ]
    END AS change_array
  FROM combined
)
SELECT
  actor, 
  actor_id,
  quality_class, 
  ca.is_active, 
  ca.start_date, 
  ca.end_date, 
  current_year
FROM changes
CROSS JOIN UNNEST(change_array) as ca