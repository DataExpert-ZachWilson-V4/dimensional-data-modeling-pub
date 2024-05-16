INSERT INTO ovoxo.actors_history_scd
WITH
  previous_year_scd AS (
    SELECT *
    FROM ovoxo.actors_history_scd
    WHERE current_year = 2019
  ),

  current_year_scd AS (
    SELECT *
    FROM ovoxo.actors
    WHERE current_year = 2020
  ),

  previous_current_combined AS ( -- similar idea to cummulative table design
    SELECT
      COALESCE(py.actor_name, cy.actor_name) AS actor_name, -- COALESCE so we don't have nulls for new players in cy
      COALESCE(py.actor_id, cy.actor_id) AS actor_id,
      COALESCE(py.start_date, cy.current_year) AS start_date,
      COALESCE(py.end_date, cy.current_year) AS end_date,
      CASE
        WHEN py.is_active <> cy.is_active OR py.quality_class <> cy.quality_class THEN 1 -- check of there is a change between py and cy for is_active or quality_class
        WHEN py.is_active = cy.is_active AND py.quality_class = cy.quality_class THEN 0 
      END AS did_change, -- records with nulls means they didn't change, we have no new records for them, it's closed
      py.is_active AS is_active_previous_year,
      cy.is_active AS is_active_current_year,
      py.quality_class AS quality_class_previous_year,
      cy.quality_class AS quality_class_current_year,
      2020 AS current_year
    FROM previous_year_scd py
    FULL OUTER JOIN current_year_scd cy ON py.actor_id = cy.actor_id -- FULL OUTER JOIN accounts for new players in cy
        AND py.end_date + 1 = cy.current_year --- match on only records that have the potential to change, records that can't change, we don't have to worry about
  ),

  changes AS (
    SELECT
      actor_name,
      actor_id,
      current_year,
      CASE
        WHEN did_change = 0 --- if no change, extend end_date of last record for cy
          THEN ARRAY[CAST(ROW(is_active_previous_year, quality_class_previous_year, start_date, end_date + 1) AS ROW(is_active boolean, quality_class varchar, start_date integer, end_date integer))]
        WHEN did_change = 1 -- if change, create new record for cy, last record of py will be end dated, older records will stay same for cy, one new record will be created
          THEN ARRAY[CAST(ROW(is_active_previous_year, quality_class_previous_year, start_date, end_date) AS ROW(is_active boolean, quality_class varchar, start_date integer, end_date integer)),
                      CAST(ROW(is_active_current_year, quality_class_current_year, current_year, current_year) AS ROW(is_active boolean, quality_class varchar, start_date integer, end_date integer))]
        WHEN did_change IS NULL --- if records not matched, copy over py records as is to cy
          THEN ARRAY[CAST(ROW(COALESCE(is_active_previous_year, is_active_current_year), COALESCE(quality_class_previous_year, quality_class_current_year), start_date, end_date) AS ROW(is_active boolean, quality_class varchar, start_date integer, end_date integer))]
      END AS change_array
    FROM previous_current_combined
  )

SELECT
  actor_name,
  actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST (change_array) AS arr