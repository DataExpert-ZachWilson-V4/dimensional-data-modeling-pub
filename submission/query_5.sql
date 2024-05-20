-- This query inserts actor data into the vpsjul8468082.actors_history_scd table.
-- It compares the current year's data with the previous year's data to identify changes in the quality_class and active status.
-- The query generates rows for changes, keeping track of start and end dates, and creates new rows for the current year if changes are detected.
-- The result is inserted into the actors_history_scd table.


INSERT INTO vpsjul8468082.actors_history_scd
-- Get last year's SCD records
WITH last_year_scd AS (
  SELECT * FROM vpsjul8468082.actors_history_scd
  WHERE current_year = 2021
),
-- Get this year's records
this_year_scd AS (
  SELECT * FROM richiesingh.actors
  WHERE current_year = 2022
),
-- Combine records from last year and this year, and evaluate changes in is_active and quality_class
combined AS (
  SELECT 
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.start_date, ty.current_year) AS start_date,
    COALESCE(ly.end_date, ty.current_year) AS end_date,
    CASE
      WHEN ly.is_active != ty.is_active OR ly.quality_class != ty.quality_class THEN 1
      WHEN ly.is_active = ty.is_active AND ly.quality_class = ty.quality_class THEN 0
    END AS did_change,
    ly.is_active AS is_active_last_year,
    ty.is_active AS is_active_current_year,
    ly.quality_class AS quality_class_last_year,
    ty.quality_class AS quality_class_current_year,
    2022 AS current_year
  FROM last_year_scd AS ly
  FULL OUTER JOIN this_year_scd AS ty
  ON ly.actor = ty.actor
  AND ly.end_date + 1 = ty.current_year
),
-- Create a row of ARRAY type based on did_change
changes AS (
  SELECT 
    actor,
    current_year,
    CASE 
      WHEN did_change = 0 THEN 
        ARRAY[
          CAST(ROW(
            quality_class_last_year,
            is_active_last_year,
            start_date,
            end_date + 1
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          ))
        ]
      WHEN did_change = 1 THEN 
        ARRAY[
          CAST(ROW(
            quality_class_last_year,
            is_active_last_year,
            start_date,
            end_date + 1
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          )),
          CAST(ROW(
            quality_class_current_year,
            is_active_current_year,
            current_year,
            current_year
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          ))
        ]
      WHEN did_change IS NULL THEN 
        ARRAY[
          CAST(ROW(
            COALESCE(quality_class_last_year, quality_class_current_year),
            COALESCE(is_active_last_year, is_active_current_year),
            start_date,
            end_date
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          ))
        ]
    END AS change_array 
  FROM combined 
)
-- Unnest the change_array to insert into the SCD table
SELECT
  actor,
  c_arr.quality_class,
  c_arr.is_active,
  c_arr.start_date,
  c_arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST(change_array) AS c_arr
