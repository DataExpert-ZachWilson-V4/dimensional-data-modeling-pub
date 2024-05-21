    -- Subqueries:
    --     last_year_scd: Selects all records from alia.actors_history_scd where current_year is 2001.
    --     current_year_scd: Selects all records from alia.actors where current_year is 2002.
    --     combined: Merges data from last_year_scd (ly) and current_year_scd (cy). Determines if there are changes in is_active or quality_class between the two years, creating a flag did_change.

    -- Change Detection:
    --     changes: Based on the did_change flag, constructs arrays (change_array) representing periods of no change, changes, or new entries, including the corresponding start_date and end_date.

    -- Main logic:
    --     Unnests the change_array to produce individual records for insertion into alia.actors_history_scd.


INSERT INTO
   alia.actors_history_scd
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
       alia.actors_history_scd
    WHERE
      current_year = 2001
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      alia.actors
    WHERE
      current_year = 2002
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
      CASE
        WHEN ly.is_active <> cy.is_active or ly.quality_class <> cy.quality_class THEN 1
        WHEN ly.is_active = cy.is_active and ly.quality_class = cy.quality_class THEN 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      ly.quality_class AS quality_class_last_year,
      cy.quality_class AS quality_class_this_year,
      2002 AS current_year
    FROM
      last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor
      AND ly.end_date + 1 = cy.current_year
  ),
  changes AS (
    SELECT
      actor,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              quality_class_last_year,
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class VARCHAR,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(quality_class_last_year,is_active_last_year, start_date, end_date) AS ROW(
              quality_class VARCHAR,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          ),
          CAST(
            ROW(
              quality_class_this_year,
              is_active_this_year,
              current_year,
              current_year
            ) AS ROW(
              quality_class VARCHAR,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(quality_class_last_year, quality_class_this_year),
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW(
              quality_class VARCHAR,
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
  CROSS JOIN UNNEST (change_array) AS arr