INSERT INTO luiscoelho37431.actors_history_scd
WITH last_year_scd AS (
  -- Select all records from the actors_history_scd table where the current_year is 1918
  SELECT *
  FROM luiscoelho37431.actors_history_scd
  WHERE current_year = 1918
),
current_year_scd AS (
  -- Select all records from the actors table where the current_year is 1919
  SELECT *
  FROM luiscoelho37431.actors
  WHERE current_year = 1919
),
combined AS (
  -- Combine the records from last_year_scd and current_year_scd using a full outer join
  SELECT
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
    COALESCE(ly.start_date, cy.current_year) AS start_date,
    COALESCE(ly.end_date, cy.current_year) AS end_date,
    CASE
      -- Check if the is_active value changed between last year and this year
      WHEN ly.is_active <> cy.is_active THEN 1
      WHEN ly.is_active = cy.is_active THEN 0
    END AS did_change,
    ly.is_active AS is_active_last_year,
    cy.is_active AS is_active_this_year,
    1919 AS current_year
  FROM last_year_scd AS ly
  FULL OUTER JOIN current_year_scd AS cy ON ly.actor_id = cy.actor_id
    AND ly.end_date + 1 = cy.current_year
),
changes AS (
  -- Create an array of change records based on the did_change value
  SELECT
    actor_id,
    quality_class,
    CASE
      -- If there was no change, create an array with a single record
      WHEN did_change = 0 THEN ARRAY[
        CAST(
          ROW(
            is_active_last_year,
            start_date,
            end_date + 1
          ) AS ROW(
            is_active boolean,
            start_date integer,
            end_date integer
          )
        )
      ]
      -- If there was a change, create an array with two records
      WHEN did_change = 1 THEN ARRAY[
        CAST(
          ROW(is_active_last_year, start_date, end_date) AS ROW(
            is_active boolean,
            start_date integer,
            end_date integer
          )
        ),
        CAST(
          ROW(
            is_active_this_year,
            current_year,
            current_year
          ) AS ROW(
            is_active boolean,
            start_date integer,
            end_date integer
          )
        )
      ]
      -- If did_change is NULL, create an array with a single record using the appropriate is_active value
      -- This section of code is used to handle the case when the 'did_change' column is NULL.
      -- It creates an array containing a single row, where the values are casted to the specified data types.
      -- The purpose of the CAST function is to convert the values in the row to the desired data types.
      -- In this case, the row contains three columns: 'is_active', 'start_date', and 'end_date',
      -- which are casted to boolean, integer, and integer data types respectively.

      WHEN did_change IS NULL THEN ARRAY[
        CAST(
          ROW(
            COALESCE(is_active_last_year, is_active_this_year),
            start_date,
            end_date
          ) AS ROW(
            is_active boolean,
            start_date integer,
            end_date integer
          )
        )
      ]
    END AS change_array,
    current_year
  FROM combined
)
-- Select the necessary columns from the exploded array
SELECT
  actor_id,
  quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
-- Performs a CROSS JOIN with the UNNEST function on the 'change_array' column, 
-- or "explodes" the array, creating a new row for each element in the array.
CROSS JOIN UNNEST(change_array) AS arr