INSERT INTO ykshon52797255.actors_history_scd

WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      ykshon52797255.actors_history_scd
    WHERE
      current_year = 2021
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      ykshon52797255.actors
    WHERE
      current_year = 2022
  ),

  combined AS (
    SELECT
      COALESCE(ls.actor, cs.actor) AS actor,
      CASE 
        WHEN ls.quality_class <> cs.quality_class AND ls.is_active <> cs.is_active THEN 1
        WHEN ls.quality_class = cs.quality_class AND ls.is_active = cs.is_active THEN 0
      END AS did_change,
      ls.quality_class AS quality_class_last_year,
      cs.quality_class AS quality_class_this_year,
      ls.is_active AS is_active_last_year,
      cs.is_active AS is_active_this_year,
      COALESCE(ls.start_date, cs.current_year) AS start_date,
      COALESCE(ls.end_date, cs.current_year) AS end_date,
      2022 AS current_year
    FROM
      last_year_scd ls
      FULL OUTER JOIN current_year_scd cs ON ls.actor = cs.actor
      AND ls.end_date + 1 = cs.current_year
  ),

  changes AS (
    SELECT
      actor,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(quality_class_last_year, is_active_last_year, start_date, end_date + 1) AS 
            ROW(quality_class VARCHAR, is_active boolean, start_date integer, end_date integer))
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(quality_class_last_year, is_active_last_year, start_date, end_date) AS 
            ROW(quality_class VARCHAR, is_active boolean, start_date integer, end_date integer)),
          CAST(
            ROW(quality_class_this_year, is_active_this_year, start_date, current_year ) AS 
            ROW(quality_class VARCHAR, is_active boolean, start_date integer, end_date integer))
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(COALESCE(quality_class_last_year, quality_class_this_year), COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS 
            ROW(quality_class VARCHAR, is_active boolean, start_date integer, end_date integer))
        ]
      END AS change_array
    FROM
      combined
  )
  
SELECT
  actor,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr


/*
Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by 
combining the previous year's SCD data with the new incoming data from the actors table for this year.

YOUKANG'S ORIGINAL DRAFT

INSERT INTO ykshon52797255.actors_history_scd

WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      ykshon52797255.actors_history_scd
    WHERE
      current_year = 2021
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      ykshon52797255.actors_history_scd
    WHERE
      current_year = 2022
  ),

  -- tracks the changes that are made from last year and this year's quality class
  -- and is_active variable
  combined AS (
    SELECT
      COALESCE(ls.actor, cs.actor) AS actor,
      COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
  -- instead of keeping the quality class did change and active did change separate, combine them into single
  -- did-change flag
      CASE 
        WHEN ls.quality_class <> cs.quality_class THEN 1
        WHEN ls.quality_class = cs.quality_class THEN 0
      END AS quality_class_did_change,
      ls.quality_class AS quality_class_last_year,
      cs.quality_class AS quality_class_this_year,
      CASE
        WHEN ls.is_active <> cs.is_active THEN 1
        WHEN ls.is_active = cs.is_active THEN 0
      END AS active_did_change,
      ls.is_active AS is_active_last_year,
      cs.is_active AS is_active_this_year,
      COALESCE(ls.start_date, cs.start_date) AS start_date,
      COALESCE(ls.end_date, cs.end_date) AS end_date,
      2022 AS current_year
    FROM
      last_year_scd ls
      FULL OUTER JOIN current_year_scd cs ON ls.actor_id = cs.actor_id
      AND ls.end_date + 1 = cs.current_year
  ),

  -- creates an array depending on changes that are being made on active and quality class
  changes AS (
    SELECT
      actor,
      actor_id,
      current_year,
      CASE
        -- when is active doesn't change, we want to grab last year's active status but add 1 to end date to indicate
        -- up to this year
        WHEN active_did_change = 0 THEN ARRAY[
          CAST(
            ROW(is_active_last_year, start_date, end_date + 1) AS 
            ROW(is_active boolean, start_date integer, end_date integer))
        ]
        -- when is active has changed, we want to grab last year's active status
        WHEN active_did_change = 1 THEN ARRAY[
          CAST(
            ROW(is_active_last_year, start_date, end_date) AS 
            ROW(is_active boolean, start_date integer, end_date integer)),
          CAST(
            ROW( is_active_this_year, start_date, current_year ) AS 
            ROW( is_active boolean, start_date integer, end_date integer))
        ]
        -- when is active is not available, then we coalesce last year and this year's active status
        WHEN active_did_change IS NULL THEN ARRAY[
          CAST(
            ROW( COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS 
            ROW(is_active boolean, start_date integer, end_date integer))
        ]
      END AS change_array_active,
    CASE
        -- look at logic for is_active
        WHEN quality_class_did_change = 0 THEN ARRAY[
          CAST( 
                ROW(quality_class_this_year, start_date, end_date + 1) AS 
                ROW( quality_class VARCHAR, start_date integer, end_date integer))
        ]
        WHEN quality_class_did_change = 1 THEN ARRAY[
          CAST(
            ROW(quality_class_last_year, start_date, end_date) AS 
            ROW(quality_class VARCHAR, start_date integer, end_date integer)),
          CAST(
            ROW(quality_class_this_year, start_date, current_year) AS 
            ROW(quality_class VARCHAR, start_date integer, end_date integer))
        ]
        WHEN quality_class_did_change IS NULL THEN ARRAY[
          CAST(
            ROW(COALESCE(quality_class_last_year, quality_class_this_year), start_date, end_date) AS 
            ROW(quality_class VARCHAR, start_date integer, end_date integer))
        ]
      END AS change_array_quality
    FROM
      combined
  )
  
SELECT
  actor,
  actor_id,
  arr_quality.quality_class,
  arr_active.is_active,
  arr_active.start_date,
  arr_active.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array_active) AS arr_active
  CROSS JOIN UNNEST (change_array_quality) AS arr_quality
*/
