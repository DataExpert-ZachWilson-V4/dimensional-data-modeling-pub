INSERT INTO changtiange199881320.actors_history_scd

-- SCD Incremental Backfill, populate a single year's data by combining the previous
-- year's SCD data with the new incoming data from the table for this year. 

WITH last_year_scd AS(
    SELECT * 
    FROM changtiange199881320.actors_history_scd
    WHERE current_year = 2020
), 
current_year_scd AS (
    SELECT * 
    FROM changtiange199881320.actors
    WHERE current_year = 2021
), 
combined AS(
    SELECT 
        COALESCE(ly.actor, cy.actor) AS actor,
        COALESCE(ly.start_date, cy.current_year) AS start_date,
        COALESCE(ly.end_date, cy.current_year) AS end_date, 
        CASE
            WHEN ly.is_active <> cy.is_active THEN 1
            WHEN ly.is_active = cy.is_active THEN 0
        END AS did_change, 
        ly.is_active AS is_active_last_year, 
        cy.is_active AS is_active_this_year,
        2021 AS current_year
    FROM 
        last_year_scd ly
    FULL OUTER JOIN 
        current_year_scd cy
    ON 
        ly.actor = cy.actor AND ly.end_date + 1 = cy.current_year
), 
changes AS (
    SELECT
        actor,
        current_year,
        CASE 
            WHEN did_change = 0 THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, start_date, end_date + 1) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
            WHEN did_change = 1 THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, start_date, end_date) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                ),
                CAST(
                    ROW(is_active_this_year, current_year, current_year) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
            WHEN did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(COALESCE(is_active_last_year, is_active_this_year), 
                        start_date, end_date) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
        END AS change_array
    FROM
        combined
)
SELECT
    actor, arr.is_active, arr.start_date,
    arr.end_date, current_year
FROM
    changes
CROSS JOIN 
    UNNEST (change_array) AS arr
