INSERT INTO mposada.actors_history_scd
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      mposada.actors_history_scd
    WHERE
      current_year = 1918  -- Select records from the previous year
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      mposada.actors
    WHERE
      current_year = 1919  -- Select records from the current year, the idea of this query is to populate actors_history_scd with new coming data after out backfill, since I initially backfilled until 1918, 1919 will be the new year
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,  -- Use actor name from either last year or this year
      COALESCE(ly.actor_id, cy.actor_id) AS actor_id,  -- Use actor ID from either last year or this year
      COALESCE(ly.start_date, cy.current_year) AS start_date,  -- Use start date from last year or current year
      COALESCE(ly.end_date, cy.current_year) AS end_date,  -- Use end date from last year or current year
      CASE
        WHEN (ly.is_active <> cy.is_active) OR
           (ly.quality_class <> cy.quality_class)  THEN 1  -- Check for changes in active status or quality class, this is so that when we group by later we get dimensions that didnt change in one row with their corresponding star and end date, by adding one when it changes it wont group them together
        WHEN (ly.is_active = cy.is_active) AND
            (ly.quality_class = cy.quality_class) THEN 0  -- No changes in active status or quality class
      END AS did_change,
      ly.is_active AS is_active_last_year,  -- Active status from last year
      cy.is_active AS is_active_this_year,  -- Active status from this year
      ly.quality_class AS quality_class_last_year,  -- Quality class from last year
      cy.quality_class AS quality_class_this_year,  -- Quality class from this year
      1919 AS current_year  -- Set the current year to 1919
    FROM
      last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor  -- Join on actor
      AND ly.end_date + 1 = cy.current_year  -- current year
  ),
  changes AS (
    SELECT
      actor,
      actor_id,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              quality_class_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(is_active_last_year,
            quality_class_last_year, 
            start_date, 
            end_date) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          ),
          CAST(
            ROW(
              is_active_this_year,
              quality_class_this_year,
              current_year,
              current_year
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              COALESCE(quality_class_last_year, quality_class_this_year),
              start_date,
              end_date
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
      END AS change_array  -- Create an array of changes
    FROM
      combined
  )
SELECT
  actor,
  actor_id,
  arr.quality_class,  -- Extract quality class from the array
  arr.is_active,  -- Extract active status from the array
  arr.start_date,  -- Extract start date from the array
  arr.end_date,  -- Extract end date from the array
  current_year
FROM
  changes
  CROSS JOIN UNNEST(change_array) AS arr  -- Unnest the array to get individual rows

