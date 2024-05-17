INSERT INTO
  derekleung.actors_history_scd
-- CTE layers:
-- Situation: SCD up till 2020 is already backfilled, now backfilling 2021 incrementally
-- last_year_scd and current_year_scd: SCD table from most recent edit and table of current state of attributes
-- combined: join the 2 dataframes, define did_change as the reference variable for updating mechanism in changes
-- changes: column change_array to capture all cumulative data in SCD type 2 up to 2021
-- SELECT: explode change_array back in conjunction with change table
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      derekleung.actors_history_scd
    WHERE
      current_year = 2020
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      derekleung.actors
    WHERE
      current_year = 2021
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
  --note we did not put else in the case when to facilitate new actors being defined as NULL for did_change
      CASE
        WHEN ly.is_active <> cy.is_active THEN 1
        WHEN ly.is_active = cy.is_active AND ly.quality_class <> cy.quality_class THEN 1
        WHEN ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class THEN 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      ly.quality_class AS quality_class_last_year,
      cy.quality_class AS quality_class_this_year,
      2021 AS current_year
    FROM
      last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor_id = cy.actor_id
      AND ly.end_date + 1 = cy.current_year
  ),
  changes AS (
    SELECT
      actor_id,
      actor,
      current_year,
--According to did_change = 0 (old actors that did not change) / 1 (old actors that changed) / NULL (new actors)
--Note by definition it is impossible for an old actor to not have 2021 data in the actors_table unless explicitly filtered out
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              quality_class_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              is_active boolean,
              quality_class varchar(9),
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              quality_class_last_year,
              start_date,
              end_date
            ) AS ROW(
              is_active boolean,
              quality_class varchar(9),
              start_date integer,
              end_date integer
            )
          ),
                    CAST(
            ROW(
              is_active_this_year,
              quality_class_this_year,
              current_year,
              current_year
            ) AS ROW(
              is_active boolean,
              quality_class varchar(9),
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              COALESCE(quality_class_last_year, quality_class_this_year),
               start_date,
              end_date
            ) AS ROW(
              is_active boolean,
              quality_class varchar(9),
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
  actor_id,
  actor,
  arr.is_active,
  arr.quality_class,
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr
