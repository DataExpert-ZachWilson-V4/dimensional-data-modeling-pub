WITH
  last_year_scd AS (
    SELECT 
      actor,
      is_active,
      start_date,
      end_date,
      quality_class
    FROM sravan.actors_history_scd
    WHERE current_year = 2020
  ),
  current_year_scd AS (
    SELECT 
      actor,
      is_active,
      current_year,
      quality_class
    FROM sravan.actors
    WHERE current_year = 2021
  ),
  combined AS (
    SELECT 
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.start_date, cy.current_year) AS start_year,
      COALESCE(ly.end_date, cy.current_year) AS end_year,
      CASE 
        WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
        ELSE 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      ly.quality_class AS quality_class_last_year,
      cy.quality_class AS quality_class_this_year,
      2021 AS current_year
    FROM last_year_scd ly
    FULL OUTER JOIN current_year_scd cy
    ON ly.actor = cy.actor
  ),
  changes AS (
    SELECT 
      actor,
      CASE 
        WHEN did_change = 0 THEN 
          ARRAY[
            CAST(
              ROW(is_active_last_year, start_year, end_year + 1, quality_class_last_year) AS ROW(
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                quality_class VARCHAR
              )
            )
          ] 
        WHEN did_change = 1 THEN 
          ARRAY[
            CAST(
              ROW(is_active_last_year, start_year, end_year, quality_class_last_year) AS ROW(
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                quality_class VARCHAR
              )
            ),
            CAST(
              ROW(is_active_this_year, current_year, current_year, quality_class_this_year) AS ROW(
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                quality_class VARCHAR
              )
            )
          ]
        WHEN did_change IS NULL THEN 
          ARRAY[
            CAST(
              ROW(
                COALESCE(is_active_last_year, is_active_this_year), 
                start_year, 
                end_year, 
                COALESCE(quality_class_last_year, quality_class_this_year)
              ) AS ROW(
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                quality_class VARCHAR
              )
            )
          ]
      END AS change_array
    FROM combined
  )
SELECT 
  actor,
  arr.is_active,
  arr.start_year,
  arr.end_year,
  arr.quality_class,
  2021 AS current_year
FROM changes
CROSS JOIN UNNEST(change_array) AS arr;
