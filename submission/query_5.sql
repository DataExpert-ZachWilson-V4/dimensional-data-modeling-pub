INSERT INTO actors_history_scd
-- CTE to select records from actors_history_scd from previoys year
WITH last_year_scd AS (
  SELECT *
  FROM actors_history_scd
  WHERE current_year = 2021
),
-- CTE to select records from actors from current year
current_year_scd AS (
  SELECT *
  FROM actors
  WHERE current_year = 2022
),
-- CTE to combine data from last year and current year
combined_cte AS (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,  -- Use the actor from last_year or current_year
    COALESCE(ly.start_date, cy.current_year) AS start_date,  -- start_date from last_year or current year
    COALESCE(ly.end_date, cy.current_year) AS end_date,  -- end_date from last_year or current year
    ly.is_active AS is_active_last_year,  -- is_active status from last year
    cy.is_active AS is_active_current_year,  -- is_active status from current year
    ly.quality_class AS quality_class_last_year,  -- quality_class from last year
    cy.quality_class AS quality_class_current_year,  -- quality_class from current year
    -- Determine if there was a change in is_active or quality_class
    CASE
      WHEN 
        ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class 
        THEN 0  -- No change
      WHEN ly.is_active != cy.is_active THEN 1  -- Change in is_active
      WHEN ly.quality_class != cy.quality_class THEN 1  -- Change in quality_class
    END AS did_change,
    2022 AS current_year  -- Set the current year
  FROM last_year_scd ly
  FULL OUTER JOIN current_year_scd cy ON
    ly.actor = cy.actor
    AND ly.end_date + 1 = cy.current_year  -- Join on actor and continuous year
),
-- CTE to create an array of changes for each actor
changes_cte AS (
  SELECT
    actor,
    current_year,
    -- Create an array of changes based on whether there was a change
    CASE
      WHEN did_change = 0
       -- No change, extend the end_date
        THEN ARRAY[
          CAST(
            ROW(is_active_last_year, quality_class_last_year, start_date, end_date + 1) AS ROW(
              is_active BOOLEAN,
              quality_class VARCHAR,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      WHEN did_change = 1
        -- Change detected: create two rows, one for last year's state, and one for the current year's state
        THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              quality_class_last_year, 
              start_date,
              end_date
            ) AS ROW(
              is_active BOOLEAN,
              quality_class VARCHAR,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW(
              is_active_current_year,
              quality_class_current_year,
              current_year,
              current_year
            ) AS ROW(
              is_active BOOLEAN,
              quality_class VARCHAR,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      WHEN did_change IS NULL
      -- Handle cases where no data is available for either last year or current year
        THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_current_year, is_active_last_year),
              COALESCE(quality_class_current_year, quality_class_last_year),
              start_date,
              end_date
            ) AS ROW(
              is_active BOOLEAN,
              quality_class VARCHAR,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
    END AS change_array
  FROM combined_cte
)
-- Select and unnest the array of changes to insert into the target table
SELECT
  changes_cte.actor,
  arr.is_active,
  arr.quality_class,
  arr.start_date,
  arr.end_date,
  changes_cte.current_year
FROM 
  changes_cte
  CROSS JOIN UNNEST (change_array) AS arr;  -- Unnest the change array

-- Testing the output table
-- SELECT *
-- FROM actors_history_scd
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year, start_date
