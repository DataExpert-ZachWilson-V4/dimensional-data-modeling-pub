INSERT INTO bgar.actors_history_scd
WITH last_year_scd AS (
  SELECT *
  FROM bgar.actors_history_scd
  WHERE current_year = 2000
),
current_year_scd AS (
  SELECT *
  FROM bgar.actors
  WHERE current_year = 2001
),
combined AS (
SELECT
  COALESCE(ly.actor, cy.actor) AS actor,
  COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
  COALESCE(ly.start_date, cy.current_year) AS start_date,
  COALESCE(ly.end_date, cy.current_year) AS end_date,
  CASE 
    WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
    WHEN ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class THEN 0
  END AS did_change,
  ly.is_active AS is_active_last_year,
  cy.is_active AS is_active_this_year,
  2001 AS current_year
FROM last_year_scd ly
FULL OUTER JOIN current_year_scd cy
ON ly.actor = cy.actor
AND ly.end_date + 1 = cy.current_year
),
changes AS (
SELECT 
  actor,
  quality_class,
  current_year,
  CASE WHEN did_change = 0 
      THEN ARRAY[
      CAST(ROW(is_active_last_year, start_date, end_date + 1) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
      ]
      WHEN did_change = 1
      THEN ARRAY[
        CAST(ROW(is_active_last_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
        CAST(ROW(is_active_this_year, current_year, current_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
        ]
    WHEN did_change IS NULL
    THEN ARRAY[CAST(ROW(COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
  ]
  END as change_array
FROM combined
)
SELECT
actor,
quality_class, 
arr.is_active,
arr.start_date,
arr.end_date,
current_year
FROM changes
CROSS JOIN UNNEST(change_array) as arr
