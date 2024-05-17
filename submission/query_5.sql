INSERT INTO actors_history_scd
WITH
  -- Represents history till last year's SCD data
  last_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 2000
  ),
  -- Represents upcoming year's SCD data to be populated
  current_year_scd AS (
    SELECT
      *,
      CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_int 
    FROM
      actors
    WHERE
      current_year = 2001
  ),
  -- Combining records from last_year_scd and current_year_scd
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      ly.quality_class AS quality_class_last_year,
      cy.quality_class AS quality_class_this_year,
      ly.is_active AS is_active_last_year,
      cy.is_active_int AS is_active_this_year,
      ly.start_date AS start_date_last_year,
      cy.current_year AS start_date_this_year,
      ly.end_date AS end_date_last_year,
      cy.current_year AS end_date_this_year,
      CASE
        WHEN (ly.is_active <> cy.is_active_int) OR (cy.quality_class <> ly.quality_class) THEN 1
        ELSE 0
      END AS did_change,
      2001 AS current_year
      FROM last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor AND ly.end_date + 1 = cy.current_year
  ),
  -- Representing the change in status of active/quality class
  changes AS (
    SELECT
      actor,
      current_year,
      CASE
        -- no change to quality_class or is_active
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              quality_class_last_year,
              is_active_last_year,
              start_date_last_year,
              end_date_last_year
            ) AS ROW(
              quality_class VARCHAR,
              is_active INTEGER,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        -- at least one change to quality_class or is_active
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(
              quality_class_last_year,
              is_active_last_year,
              start_date_last_year,
              end_date_last_year
            ) AS ROW(
              quality_class VARCHAR,
              is_active INTEGER,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW(
              quality_class_this_year,
              is_active_this_year,
              start_date_this_year,
              end_date_this_year
            ) AS ROW(
              quality_class VARCHAR,
              is_active INTEGER,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(quality_class_last_year, quality_class_this_year),
              COALESCE(is_active_last_year, is_active_this_year),
              COALESCE(start_date_last_year, start_date_this_year),
              COALESCE(end_date_last_year, end_date_this_year)
            ) AS ROW(
              quality_class VARCHAR,
              is_active INTEGER,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      END AS change_array
    FROM combined
  )

SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr
 
