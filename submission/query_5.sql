-- Insert updated and new records into the actors_history_scd table
INSERT INTO raniasalzahrani.actors_history_scd (
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
)
-- Retrieves all entries from the history table for the previous year (2002)
WITH last_year_scd AS (
  SELECT * 
  FROM raniasalzahrani.actors_history_scd
  WHERE current_year = 2002
),
-- Fetches all current year (2003) data from the actors table for processing
current_year_scd AS (
  SELECT 
    actor,
    quality_class,
    is_active,
    DATE_TRUNC('day', CAST('2003-01-01' AS DATE)) AS start_date,
    DATE '9999-12-31' AS end_date,
    2003 AS current_year
  FROM raniasalzahrani.actors
  WHERE current_year = 2003
),
-- Merges last year's historical data with this year's data, assessing changes in 'is_active' and 'quality_class'
combined AS (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,  -- Use current year actor if available, otherwise last year's actor
    COALESCE(ly.start_date, DATE_TRUNC('day', CAST('2003-01-01' AS DATE))) AS start_date,  -- Use current year start date if available, otherwise last year's start date
    COALESCE(ly.end_date, DATE_TRUNC('day', CAST('2003-12-31' AS DATE))) AS end_date,  -- Use current year end date if available, otherwise last year's end date
    -- General flag indicating any change in 'is_active' or 'quality_class'
    CASE 
      WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
      WHEN ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class THEN 0
    END AS did_change,
    ly.is_active AS is_active_last_year,  -- Last year's is_active status
    cy.is_active AS is_active_this_year,  -- Current year's is_active status
    ly.quality_class AS quality_last_year,  -- Last year's quality_class
    cy.quality_class AS quality_this_year,  -- Current year's quality_class
    2003 AS current_year  -- Current year
  FROM last_year_scd ly
  FULL OUTER JOIN current_year_scd cy
  ON ly.actor = cy.actor
  AND ly.end_date + INTERVAL '1' DAY = DATE_TRUNC('day', CAST('2003-01-01' AS DATE))
),
-- Generates arrays of historical records, updated or unchanged, based on detected changes
changes AS (
  SELECT 
    actor, 
    current_year,
    -- Constructs an array of historical rows based on whether there was a change or not
    CASE WHEN did_change = 0
    THEN ARRAY[
      CAST(ROW(
        is_active_last_year,
        quality_last_year,
        start_date,
        end_date + INTERVAL '1' DAY)
      AS ROW(
        is_active BOOLEAN, 
        quality_class VARCHAR, 
        start_date DATE, 
        end_date DATE))
    ]
    WHEN did_change = 1
    THEN ARRAY[
      CAST(ROW(
        is_active_last_year,
        quality_last_year,
        start_date,
        end_date)
      AS ROW(
        is_active BOOLEAN,
        quality_class VARCHAR,
        start_date DATE,
        end_date DATE)),
      CAST(ROW(
        is_active_this_year,
        quality_this_year,
        DATE_TRUNC('day', CAST('2003-01-01' AS DATE)),
        DATE '9999-12-31')
      AS ROW(
        is_active BOOLEAN,
        quality_class VARCHAR,
        start_date DATE,
        end_date DATE))
    ]
    WHEN did_change IS NULL
    THEN ARRAY[
      CAST(ROW(
        COALESCE(is_active_last_year, is_active_this_year),
        COALESCE(quality_last_year, quality_this_year),
        start_date,
        end_date)
      AS ROW(
        is_active BOOLEAN, 
        quality_class VARCHAR, 
        start_date DATE, 
        end_date DATE))
    ]
  END AS change_array
FROM combined)
-- Final SELECT to insert updated or unchanged historical records into the history table
SELECT 
  actor,
  arr.quality_class, 
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST(change_array) AS arr
