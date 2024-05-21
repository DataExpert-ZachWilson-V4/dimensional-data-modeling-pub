INSERT INTO tdueck66966.actors_history_scd
WITH last_year_scd AS ( --CTE for last year's scd data as the base of the build
  SELECT
    *
  FROM tdueck66966.actors_history_scd
  WHERE current_year = 2010
),
current_year_scd AS ( --CTE for the current year's data from the actors table for the incremental build to the scd history
  SELECT
    *
  FROM tdueck66966.actors
  WHERE current_year = 2011
),
combined AS ( --CTE to combine/coalesce old and new data to appropriately capture new actors when not present in historical data for the build
  SELECT
    COALESCE(ly.actor_id, cy.actor_id) as actor_id,
    COALESCE(ly.actor, cy.actor) as actor,
    COALESCE(ly.start_date, cy.current_year) as start_date,
    COALESCE(ly.end_date, cy.current_year) as end_date,
    CASE
      WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1 --checked for changes in both active years and quality class
      WHEN ly.is_active = cy.is_active OR ly.quality_class = cy.quality_class THEN 0 --checks for matches in both active years and quality class
      END as did_change,
    ly.quality_class as qc_last_year,
    cy.quality_class as qc_this_year,
    ly.is_active as is_active_last_year,
    cy.is_active as is_active_this_year,
    2011 as current_year
  FROM last_year_scd ly
    FULL OUTER JOIN current_year_scd cy ON ly.actor_id = cy.actor_id AND ly.end_date + 1 = cy.current_year
),
changes AS (
SELECT 
  actor_id,
  actor,
  current_year,
  CASE
    WHEN did_change = 0
      THEN ARRAY[
        CAST(ROW(is_active_last_year, qc_last_year, start_date, end_date + 1) as ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INT, end_date INT))
      ] --maintain last year values and add a year to end_date
    WHEN did_change = 1
      THEN ARRAY[
        CAST(ROW(is_active_last_year, qc_last_year, start_date, end_date) as ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INT, end_date INT)),
        CAST(ROW(is_active_this_year, qc_this_year, current_year, current_year) as ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INT, end_date INT))
      ] --include both last year and current year values for the change array
    WHEN did_change IS NULL
      THEN ARRAY[
        CAST(ROW(COALESCE(is_active_last_year, is_active_this_year), COALESCE(qc_last_year, qc_this_year), start_date, end_date) as ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INT, end_date INT))
      ] --coalesce last year and current year values to ensure unchanged actor history is retained and new actors are captured with current year
    END as change_array
 FROM combined
)

SELECT
  actor_id,
  actor,
  quality_class,
  is_active,
  start_date,
  end_date,
  current_year
FROM changes
CROSS JOIN UNNEST(change_array)
