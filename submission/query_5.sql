--Actors History SCD Table Incremental Backfill Query (query_5)
--Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the 
--previous year's SCD data with the new incoming data from the actors table for this year.

--FOLLOWED DAY 2 LAB STEP BY STEP
INSERT INTO saidaggupati.actors_history_scd
--CTE for previous year's extract
WITH last_year_scd AS (
    SELECT actor,quality_class,is_active,start_date,end_date
    FROM saidaggupati.actors_history_scd WHERE current_year = 1914
),

--CTE for current year's extract 
current_year_scd AS (
    SELECT actor,quality_class,is_active,current_year 
    FROM saidaggupati.actors WHERE current_year = 1915
),
--last year+current year data load
combined AS (
    SELECT
        COALESCE(ly.actor, cy.actor) AS actor,
        ly.quality_class AS quality_class_previous_year,
        cy.quality_class AS quality_class_current_year,
        ly.is_active AS is_active_previous_year,
        cy.is_active AS is_active_current_year,
   CASE WHEN ly.quality_class <> cy.quality_class THEN 1 ELSE 0 END AS did_change,
   --Extracting YEAR from the DATE format - scd table. We do this to maintain the COALESCE function as the other arg is year.
        COALESCE(EXTRACT(YEAR FROM ly.start_date),cy.current_year) AS start_date,
        COALESCE(EXTRACT(YEAR FROM ly.end_date),cy.current_year) AS end_date,
        cy.current_year
    FROM last_year_scd ly 
    FULL OUTER JOIN current_year_scd cy 
    ON ly.actor = cy.actor
),
--keep track of changes from previous year to current year
changes AS (
    SELECT
        actor,
        current_year,
  CASE
    WHEN did_change = 0 THEN ARRAY[CAST(ROW(COALESCE(is_active_previous_year, is_active_current_year),COALESCE(quality_class_previous_year, quality_class_current_year),start_date,current_year) 
    AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_date INTEGER,end_date INTEGER))]
    
    WHEN did_change = 1 AND is_active_previous_year IS NULL THEN ARRAY[CAST(ROW(is_active_current_year, quality_class_current_year,current_year,current_year) 
    AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_date INTEGER,end_date INTEGER))]
    
    WHEN did_change = 1 AND is_active_previous_year IS NOT NULL THEN ARRAY[CAST(ROW(is_active_previous_year,
    quality_class_previous_year,start_date,current_year - 1) 
    AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_date INTEGER,end_date INTEGER)),
    CAST(ROW(is_active_current_year,quality_class_current_year,current_year,current_year) 
    AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_date INTEGER,end_date INTEGER) ) ]
    
END AS change_array
FROM combined 
)


SELECT
    actor as actor,
    change_array.quality_class as quality_class,
    change_array.is_active AS is_active,
    CONCAT(CAST(change_array.start_date AS VARCHAR), '-01-01') AS start_date,
    CONCAT(CAST(change_array.end_date AS VARCHAR), '-12-31') AS end_date,
    current_year as current_year
FROM
    changes
    CROSS JOIN UNNEST(changes.change_array) AS change_array
