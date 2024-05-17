INSERT INTO actors_history_scd
WITH last_year_scd AS (
  SELECT *
  FROM actors_history_scd
  WHERE current_year = 2017
),
current_year_scd AS (
  SELECT *
  FROM actors
  WHERE current_year = 2018
),
combined_cte AS (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.start_date, cy.current_year) AS start_date,
    COALESCE(ly.end_date, cy.current_year) AS end_date,
    ly.is_active AS is_active_last_year,
    cy.is_active AS is_active_current_year,
    ly.quality_class AS quality_class_last_year,
    cy.quality_class AS quality_class_current_year,
    CASE
      WHEN 
        ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class 
        THEN 0
      WHEN ly.is_active != cy.is_active THEN 1
      WHEN ly.quality_class != cy.quality_class THEN 1
    END AS did_change,
    2018 AS current_year
  FROM last_year_scd ly
  FULL OUTER JOIN current_year_scd cy ON
    ly.actor = cy.actor
    AND ly.end_date + 1 = cy.current_year
),
changes_cte AS (
  SELECT
    actor,
    current_year,
    CASE
      WHEN did_change = 0
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
SELECT
  changes_cte.actor,
  arr.is_active,
  arr.quality_class,
  arr.start_date,
  arr.end_date,
  changes_cte.current_year
FROM 
  changes_cte
  CROSS JOIN UNNEST (change_array) AS arr

-- Testing the output table
-- SELECT *
-- FROM actors_history_scd
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year, start_date
