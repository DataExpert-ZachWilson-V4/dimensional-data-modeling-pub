-- Insert updated actor records into the historical SCD table based on evaluations from previous and current year data.
INSERT INTO videet.actors_history_scd
WITH
  -- Extract data from the previous year to check against updates needed for the current year.
  last_year_scd AS (
    SELECT
      *,
      EXTRACT(YEAR FROM end_date) AS end_year  -- Extract the year from the end date for comparison.
    FROM
      videet.actors_history_scd
    WHERE
      EXTRACT(YEAR FROM end_date) = 2020  -- Filter records specifically from 2020.
  ),
  -- Aggregate current year data to assess changes and determine quality class based on average film ratings.
  current_year_scd AS (
    SELECT
      *,
      (year = 2021) AS is_active,  -- Identify if the record pertains to the current year.
      -- Classify the quality based on the average film rating.
      CASE
        WHEN AVG(rating) OVER (PARTITION BY actor_id) > 8 THEN 'star'
        WHEN AVG(rating) OVER (PARTITION BY actor_id) > 7 AND AVG(rating) OVER (PARTITION BY actor_id) <= 8 THEN 'good'
        WHEN AVG(rating) OVER (PARTITION BY actor_id) > 6 AND AVG(rating) OVER (PARTITION BY actor_id) <= 7 THEN 'average'
        WHEN AVG(rating) OVER (PARTITION BY actor_id) <= 6 THEN 'bad'
        ELSE 'unknown'  -- Handle cases where the average rating does not fit other categories.
      END AS quality_class
    FROM
      bootcamp.actor_films
    WHERE
      year = 2021  -- Focus on films from 2021.
  ),
  -- Combine data from last year and this year to identify necessary updates or continuations.
  combined AS (
    SELECT
      COALESCE(ly.actor_id, cy.actor_id) AS actor_id,  -- Coalesce actor IDs to ensure no nulls.
      COALESCE(ly.quality_class, cy.quality_class) AS quality_class,  -- Include the most recent quality class.
      COALESCE(ly.start_date, DATE '2021-01-01') AS start_year,  -- Default start of the season if not present.
      COALESCE(ly.end_date, DATE '2021-12-31') AS end_year,  -- Default end of the season if not present.
      -- Detect changes in active status; 1 for change, 0 for no change.
      CASE
        WHEN (ly.is_active <> cy.is_active OR ly.is_active IS NULL OR cy.is_active IS NULL) THEN 1
        ELSE 0
      END AS did_change,
      ly.is_active AS is_active_last_year,  -- Store last year's active status.
      cy.is_active AS is_active_this_year,  -- Store this year's active status.
      2021 AS current_year  -- Set the current year for combined records.
    FROM
      last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor_id = cy.actor_id
        AND ly.end_year + 1 = cy.year  -- Link records year over year.
  ),
  -- Prepare final changes for insertion, taking all detected changes into account.
  changes AS (
    SELECT
      actor_id,
      quality_class,  -- Include quality class in the final output.
      current_year,
      -- Construct arrays of data changes to be unnested in the final SELECT.
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              start_year,
              DATE_ADD('year', 1, end_year)  -- Extend the end season by one year if no changes.
            ) AS ROW(
              is_active boolean,
              start_year date,
              end_year date
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(is_active_last_year, start_year, end_year) AS ROW(
              is_active boolean,
              start_year date,
              end_year date
            )
          ),
          CAST(
            ROW(
              is_active_this_year,
              DATE '2021-01-01',
              DATE '2021-12-31'
            ) AS ROW(
              is_active boolean,
              start_year date,
              end_year date
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              start_year,
              end_year
            ) AS ROW(
              is_active boolean,
              start_year date,
              end_year date
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
-- Select the final structured data for insertion into the actors_history_scd table.
SELECT
  actor_id,
  quality_class,  -- Pass through the quality classification.
  is_active,
  start_year,
  end_year
FROM
  changes
  CROSS JOIN UNNEST(change_array) AS t(is_active, start_year, end_year) 
  -- Unnest the array of changes to extract individual records for insertion.
  -- Each row represents an actor's status either continued or updated with new information.
