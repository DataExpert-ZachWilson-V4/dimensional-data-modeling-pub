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
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
      ly.is_active AS is_active_last_year,
      cy.is_active_int AS is_active_this_year,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
      CASE
        WHEN ly.is_active <> cy.is_active_int THEN 1
        WHEN ly.is_active = cy.is_active_int THEN 0
      END AS did_change,
      2001 AS current_year
      FROM last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor AND ly.end_date + 1 = cy.current_year
  ),
  changes AS (
    SELECT
      actor,
      quality_class,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(
              is_active_this_year,
              start_date,
              end_date
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
      END AS change_array
    FROM combined
  )
 
SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr

