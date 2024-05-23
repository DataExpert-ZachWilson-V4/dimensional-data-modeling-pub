-- Actors History SCD Table Incremental Backfill Query (query_5)
-- Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD data with the new incoming data from the actors table for this year.


WITH last_year_scd AS (
  SELECT
    *
  FROM
    ttian45759.actors_history_scd 
  WHERE current_year = 2021
),

current_year AS (
  SELECT
    *
  FROM
    ttian45759.actors
  WHERE current_year = 2022
),


-- Join the two tables and decide if there are changes
row_reconciliation as (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    ly.quality_class as last_year_quality_class,
    cy.quality_class as current_year_quality_class,
    ly.is_active as last_year_is_active,
    cy.is_active as current_year_is_active,
    -- need to coalesce the if there are new values
    COALESCE(ly.start_date , cy.current_year) as start_date,
    COALESCE(ly.end_date, cy.current_year) as end_date,
    CASE
      WHEN (ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class) THEN 1
      WHEN (ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class) THEN 0
    END AS did_change,
    2022 AS current_year
  FROM
    last_year_scd ly
  FULL OUTER JOIN current_year cy
  ON ly.actor_id = cy.actor_id
  AND ly.current_year + 1 = cy.current_year
),


-- Define a CTE for changes, similar to the example solution.
changes AS (
  SELECT
    actor,
    current_year,
    CASE
      WHEN did_change = 0
        THEN ARRAY [
          CAST(
            ROW (
              last_year_quality_class,
              last_year_is_active,
              start_date,
              end_date + 1
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
              )
            )
        ]
      WHEN did_change = 1
        THEN ARRAY [
          CAST(
            ROW (
              last_year_quality_class,
              last_year_is_active,
              start_date,
              end_date
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW (
              current_year_quality_class,
              current_year_is_active,
              current_year,
              current_year
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )	
          )
        ]
      WHEN did_change IS NULL
        THEN ARRAY [
          CAST(
            ROW (
              COALESCE(last_year_quality_class, current_year_quality_class),
              COALESCE(last_year_is_active, current_year_is_active),
              start_date,
              end_date
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
              )
            )
        ]
    END AS change_array
  FROM row_reconciliation
)


  SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
  FROM changes
  CROSS JOIN UNNEST(change_array) AS arr
