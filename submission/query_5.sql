-- Inserting data into the table "nancyatienno21998.actors_history_scd"

INSERT INTO nancyatienno21998.actors_history_scd

-- Using Common Table Expressions (CTEs) to organize the query
WITH
  -- last_year_scd CTE  to fetch data from last year's records
  last_year_scd AS (
    SELECT
      *
    FROM
      nancyatienno21998.actors_history_scd
    WHERE
      current_year = 1922
  ),

  -- current_year_scd CTE to fetch data from current year's records
  current_year_scd AS (
    SELECT
      *
    FROM
      nancyatienno21998.actors
    WHERE
      current_year = 1923
  ), 

  -- CTE to combine data from last year and current year
  combined AS (
    SELECT 
      COALESCE(ly.actor, cy.actor) AS actor, 
      COALESCE(ly.start_date, cy.current_year) AS start_date, 
      COALESCE(ly.end_date, cy.current_year) AS end_date, 
      CASE WHEN ly.is_active <> cy.is_active THEN 1 
           WHEN ly.is_active = cy.is_active THEN 0 END AS did_change, 
      ly.is_active AS is_active_last_year, 
      cy.is_active AS is_active_this_year, 
      ly.quality_class AS ly_quality_class, 
      cy.quality_class AS cy_quality_class, 
      1922 as current_year 
    FROM 
      last_year_scd ly FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor 
      AND ly.end_date + 1 = cy.current_year
  ), 

  -- CTE to determine changes in actor records
  changes AS (
    SELECT 
      actor, 
      current_year, 
      CASE
          WHEN did_change = 0 THEN ARRAY[ CAST(
              ROW(
                  ly_quality_class, is_active_last_year, 
                  start_date, end_date + 1
              ) AS ROW(
                  quality_class varchar, is_active boolean, 
                  start_date integer, end_date integer
              )) ] 
          WHEN did_change = 1 THEN ARRAY[ CAST(
              ROW(
                  ly_quality_class, is_active_last_year,
                  start_date, end_date
              ) AS ROW(
                  quality_class varchar, is_active boolean,
                  start_date integer, end_date integer
              )
              ),
              CAST(
              ROW(
                  cy_quality_class, is_active_this_year,
                  start_date, start_date
              ) AS ROW(
                  quality_class varchar, is_active boolean,
                  start_date integer, end_date integer
              )
              ) ]
          WHEN did_change IS NULL THEN ARRAY[ CAST(
              ROW(
                  COALESCE(
                  ly_quality_class, cy_quality_class
                  ), 
                  COALESCE(
                  is_active_last_year, is_active_this_year
                  ), 
                  start_date, 
                  end_date
              ) AS ROW(
                  quality_class varchar, is_active boolean,
                  start_date integer, end_date integer
              )
              ) ]
          END AS change_array
    FROM
      combined
  ) 

-- Selecting the final result set
SELECT 
  actor, 
  arr.quality_class, 
  arr.is_active, 
  arr.start_date, 
  arr.end_date, 
  current_year 
FROM 
  changes CROSS JOIN UNNEST (change_array) AS arr
