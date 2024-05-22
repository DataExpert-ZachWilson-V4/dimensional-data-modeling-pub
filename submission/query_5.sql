-- Retrieve previous year's data from the actors_history_scd table
WITH previous_year_scd AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
  FROM human.actors_history_scd
  WHERE end_date IS NULL
),

-- Extract current year's data from the actors table
current_year_actors AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year
  FROM human.actors
  WHERE current_year = 2022
),

-- Combine previous year's data with current year's data and determine changes
combined AS (
  SELECT
    COALESCE(py.actor_id, cy.actor_id) AS actor_id,
    COALESCE(py.actor, cy.actor) AS actor,
    COALESCE(cy.quality_class, py.quality_class) AS quality_class,
    COALESCE(cy.is_active, py.is_active) AS is_active,
    py.current_year AS previous_year,
    cy.current_year AS current_year,
    COALESCE(py.start_date, DATE '2022-01-01') AS start_date,
    CASE
      WHEN cy.actor_id IS NULL THEN py.end_date
      ELSE COALESCE(py.end_date, DATE '2021-12-31')
    END AS end_date,
    CASE
      WHEN py.quality_class IS DISTINCT FROM cy.quality_class THEN 1
      WHEN py.is_active IS DISTINCT FROM cy.is_active THEN 1
      ELSE 0
    END AS did_change
  FROM previous_year_scd py
  FULL OUTER JOIN current_year_actors cy ON py.actor_id = cy.actor_id
),

-- Generate an array of row changes based on the comparison between previous and current year data
changes AS (
  SELECT
    actor_id,
    actor,
    CASE
      WHEN did_change = 0 THEN ARRAY[
        ROW(quality_class, is_active, start_date, DATE '2023-12-31')
      ]
      WHEN did_change = 1 THEN ARRAY[
        ROW(quality_class, is_active, start_date, end_date),
        ROW(quality_class, is_active, DATE '2022-01-01', NULL)
      ]
      ELSE ARRAY[
        ROW(quality_class, is_active, start_date, NULL)
      ]
    END AS change_array
  FROM combined
)

-- Select the desired columns and unnest the change_array to produce the final result set
SELECT
  actor_id,
  actor,
  t.quality_class,
  t.is_active,
  t.start_date AS current_year,
  t.start_date,
  t.end_date
FROM changes
CROSS JOIN UNNEST(change_array) AS t(quality_class, is_active, start_date, end_date)