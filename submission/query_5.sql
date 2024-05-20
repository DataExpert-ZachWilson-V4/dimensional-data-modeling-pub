INSERT INTO
    halloweex.actors_history_scd
WITH
  -- Step 1: Retrieve the last year's SCD data
  last_year_scd AS (
    SELECT
      actor,
      quality_class,
      start_date,
      end_date,
      current_year,
      is_current,
      is_active
    FROM
       halloweex.actors_history_scd
    WHERE
      current_year = 2021
  ),

  -- Step 2: Retrieve the current year's data
  current_year_data AS (
    SELECT
      actor,
      quality_class,
      is_active,
      2022 AS start_date,  -- Using integer to represent the year
      NULL AS end_date,
      2022 AS current_year,
      TRUE AS is_current
    FROM
       halloweex.actors
    WHERE
      current_year = 2022
  ),

  -- Step 3: Combine previous and current year data
  combined AS (
    SELECT
      COALESCE(ls.actor, cy.actor) AS actor,  -- Use actor from either last year or current year
      COALESCE(ls.start_date, cy.start_date) AS start_date,  -- Use start_date from last year or current year
      COALESCE(ls.end_date, cy.start_date - 1) AS end_date,  -- Use end_date from last year or one year before current start_date
      ls.quality_class AS quality_class_last_year,  -- Quality class from last year
      cy.quality_class AS quality_class_this_year,  -- Quality class from current year
      ls.is_active AS is_active_last_year,  -- Is active status from last year
      cy.is_active AS is_active_this_year,  -- Is active status from current year
      CASE
        WHEN ls.quality_class <> cy.quality_class OR ls.is_active <> cy.is_active THEN TRUE
        ELSE FALSE
      END AS did_change,  -- Determine if there was a change in quality class or active status
      2022 AS current_year
    FROM
      last_year_scd ls
      FULL OUTER JOIN current_year_data cy ON ls.actor = cy.actor
      AND ls.end_date + 1 = cy.current_year  -- Ensure sequential years
  ),

  -- Step 4: Identify changes and create new rows for the SCD table
  changes AS (
    SELECT
      actor,
      COALESCE(quality_class_last_year, quality_class_this_year) AS quality_class,  -- Use current quality class if it exists, otherwise use last year's
      current_year,
      CASE
        WHEN did_change = FALSE THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              start_date,
              end_date
            ) AS ROW(
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change = TRUE THEN ARRAY[
          CAST(
            ROW(is_active_last_year, start_date, end_date) AS ROW(
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW(
              is_active_this_year,
              2022,  -- Using integer to represent the year
              NULL
            ) AS ROW(
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW(
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      END AS change_array  -- Create array of changes to be unwrapped
    FROM
      combined
  )

-- Step 5: Insert the new records into the SCD table
SELECT
  actor,
  quality_class,
  arr.start_date,
  arr.end_date,
  current_year,
  arr.end_date IS NULL AS is_current,  -- Determine if the record is current
  arr.is_active
FROM
  changes
  CROSS JOIN UNNEST(change_array) AS arr;  -- Unnest the array of changes to insert each change as a new row
