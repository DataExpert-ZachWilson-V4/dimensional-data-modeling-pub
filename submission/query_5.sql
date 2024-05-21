WITH last_year_scd AS (
  SELECT * FROM denzelbrown.actors_history_scd
  WHERE current_year = 1923
),
current_year_scd AS (
  SELECT * FROM denzelbrown.actors_history_scd
  WHERE current_year = 1924
)
SELECT
  ly.actor,
  ly.start_date,
  ly.end_date, 
  CASE 
    WHEN ly.is_active <> cy.is_active THEN 1
    WHEN ly.is_active = cy.is_active THEN 0
   END as did_change
FROM last_year_scd ly
FULL OUTER JOIN current_year_scd cy
ON ly.actor = cy.actor AND ly.end_date + 1 = cy.current_year
