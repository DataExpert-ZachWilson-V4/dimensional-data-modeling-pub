-- Insert new rows into the actors_history_scd table
INSERT INTO
  actors_history_scd
-- last_year_scd CTE captures all data from actors_history_scd for the year 1914
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1914
  ),
-- current_year_scd CTE captures all data from actors_history_scd for the year 1915
  current_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1915
  ),
  combined AS (
    SELECT
      COALESCE(ls.actor, cs.actor) AS actor,
      COALESCE(ls.start_date, cs.start_date) AS start_date,
      COALESCE(ls.end_date, cs.end_date) AS end_date,
      CASE
        WHEN ls.is_active <> cs.is_active  OR ls.quality_class <> cs.quality_class
THEN 1
        WHEN ls.is_active = cs.is_active THEN 0
      END AS did_change,
      ls.is_active AS is_active_last_date,
      cs.is_active AS is_active_this_date,
      ls.quality_class AS ly_quality_class,
      cs.quality_class AS ty_quality_class,
      1915 AS current_year
    FROM
      last_year_scd ls
      FULL OUTER JOIN current_year_scd cs ON ls.actor = cs.actor
      AND ls.end_date+ 1 = cs.current_year
  ),
  changes AS (
    SELECT
      actor,
      current_year,
-- If there is no change, create an array with one element representing the updated period
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
	      ly_quality_class,
              is_active_last_date,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
-- If there is a change, create an array with two elements: the last period and the new period
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(ly_quality_class,is_active_last_date, start_date, end_date) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_season integer,
              end_season integer
            )
          ),
          CAST(
            ROW(
              ty_quality_class,
              is_active_this_date,
              current_year,
              current_year
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
 -- Handle the case where did_change is NULL by creating a single element array with COALESCE values
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(ly_quality_class, ty_quality_class),
              COALESCE(is_active_last_date, is_active_this_date),
              start_date,
              end_date
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
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
-- Unnest the change_array to separate rows for insertion into the target table
  CROSS JOIN UNNEST (change_array) AS arr(quality_class,is_active, start_date, end_date)
