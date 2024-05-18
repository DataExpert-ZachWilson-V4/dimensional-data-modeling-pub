

-- Insert the processed data into the actors_history_scd table
INSERT INTO faraanakmirzaei15025.actors_history_scd
WITH
  -- Select all records from the previous year's SCD table
  last_year_scd AS (
    SELECT
      *
    FROM
      faraanakmirzaei15025.actors_history_scd
    WHERE 
      current_year = 2021
  ),
  -- Select current year's actor data where the year is 2000
  this_year_scd AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      current_year,
      is_active
    FROM
      faraanakmirzaei15025.actors
    WHERE
      current_year = 2022
  ),
  -- Combine last year's and this year's data to determine changes
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
      CASE
        WHEN ly.quality_class = cy.quality_class AND ly.is_active = cy.is_active 
          THEN 0
        WHEN ly.quality_class <> cy.quality_class OR ly.is_active <> cy.is_active 
          THEN 1
      END AS did_change,
      ly.quality_class AS ly_quality_class,
      cy.quality_class AS cy_quality_class,
      ly.is_active AS ly_is_active,
      cy.is_active AS cy_is_active,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
      2000 AS current_year -- Fixed year, same as where clause in this_year_scd CTE
    FROM last_year_scd ly
    FULL OUTER JOIN this_year_scd cy 
    ON ly.actor_id = cy.actor_id
    AND ly.end_date + 1 = cy.current_year
  ),
  -- Constructs an array of changes based on the did_change flag from the previous CTE
  changes AS (
    SELECT
      actor,
      actor_id,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          ROW (
            ly_quality_class,
            ly_is_active,
            start_date,
            end_date + 1
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          ROW (
            ly_quality_class,
            ly_is_active,
            start_date,
            end_date
          ),
          ROW (
            cy_quality_class,
            cy_is_active,
            current_year,
            current_year
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          ROW (
            COALESCE(ly_quality_class, cy_quality_class),
            COALESCE(ly_is_active, cy_is_active),
            COALESCE(start_date, current_year),
            COALESCE(end_date, current_year)
          )
        ]
      END AS changes_array
    FROM combined
  )
-- Append the information, unnesting the array
SELECT
  actor,
  actor_id,
  ar.quality_class,
  ar.is_active,
  ar.start_date,
  ar.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST (changes_array) AS ar (quality_class, is_active, start_date, end_date)