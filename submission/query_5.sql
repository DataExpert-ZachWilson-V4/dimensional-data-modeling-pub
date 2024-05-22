-- Actors History SCD Table Incremental Backfill Query (query_5)
-- Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD data with the new incoming data from the actors table for this year.


WITH last_year_scd AS (
  SELECT
    *
  FROM
    ttian45759.actors_history_scd 
  WHERE current_year = 2005
),

current_year AS (
  SELECT
    *
  FROM
    ttian45759.actors
  WHERE current_year = 2006
),

row_reconciliation as (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    ly.quality_class as last_year_quality_class,
    cy.quality_class as current_year_quality_class,
    ly.is_active as last_year_is_active,
    cy.is_active as current_year_is_active,
    CASE
      WHEN (ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class) THEN 1
      WHEN (ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class) THEN 0
    END AS did_change,
    2006 AS current_year
  FROM
    last_year_scd ly
  FULL OUTER JOIN current_year cy
  ON ly.actor_id = cy.actor_id
  AND ly.current_year + 1 = cy.current_year
)


select * from row_reconciliation where did_change = 0