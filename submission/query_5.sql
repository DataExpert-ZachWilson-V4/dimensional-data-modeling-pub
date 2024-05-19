/*Actors History SCD Table Incremental Backfill Query (query_5)

Write an "incremental" query that can populate a single year's worth of the `actors_history_scd`
table by combining the previous year's SCD data with the new incoming data from the `actors` table for this year.
*/

INSERT INTO actors_history_scd

WITH
  last_batch AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1956
  ),

  current_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1957
  ),

  combined AS (
    SELECT
    COALESCE(lb.actor, cy.actor) AS actor,
    COALESCE(lb.actor_id, cy.actor_id) AS actor_id,
    COALESCE(lb.start_date, cy.current_year) AS start_date,
    COALESCE(lb.end_date, cy.current_year) AS end_date,
    CASE WHEN lb.end_date + 1 = cy.current_year THEN -- only changes since last partition, assuming older records weren't touched
         CASE WHEN (lb.is_active <> cy.is_active)
            OR (lb.quality_class <> cy.quality_class) THEN 1 -- when the status changed, we need a new record
            WHEN (lb.is_active = cy.is_active)
            OR (lb.quality_class = cy.quality_class) THEN 0 -- when the status hasn't changed, we need to extend the existing record
      END ELSE NULL 
      END AS did_change, -- there is a possibility of nulls... we're doing a full outer join to capture older records and new actors,
    lb.quality_class AS quality_class_last_batch,
    cy.quality_class AS quality_class_this_year,
    lb.is_active AS is_active_last_batch,
    cy.is_active AS is_active_this_year,
    1957 AS current_year
    FROM
    last_batch lb
    FULL OUTER JOIN current_year cy ON lb.actor = cy.actor
    AND lb.current_year + 1 = cy.current_year
  ),

changes AS (
  SELECT
    actor,
    actor_id,
    CASE 
        -- when the status hasn't changed, the new record will be an extension of the existing one
        WHEN did_change = 0 
        THEN ARRAY[CAST(ROW(
                quality_class_last_batch,
                is_active_last_batch,
                start_date,
                end_date + 1) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
        -- when the status changed, we copy the old record and need a new record
        WHEN did_change = 1
            THEN ARRAY[CAST(ROW(
                quality_class_last_batch,
                is_active_last_batch,
                start_date,
                end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
             CAST(ROW(
                quality_class_this_year,
                is_active_this_year,
                current_year,
                current_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
        -- new people or old records that will not change
        WHEN did_change IS NULL
        THEN ARRAY[CAST(ROW(
                COALESCE(quality_class_last_batch, quality_class_this_year),
                COALESCE(is_active_last_batch, is_active_this_year),
                start_date,
                end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
        END AS change_array
  FROM combined
  )

SELECT
    actor,
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    1957 as current_year
FROM changes
CROSS JOIN UNNEST(change_array) arr