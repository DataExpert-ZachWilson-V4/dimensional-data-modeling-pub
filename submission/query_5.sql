INSERT INTO dennisgera.actors_history_scd

WITH 

last_year AS (
    SELECT *
    FROM dennisgera.actors_history_scd
    WHERE current_year = 2021
),

current_year AS (
    SELECT *
    FROM dennisgera.actors
    WHERE current_year = 2022
),

combined AS (
    SELECT
        COALESCE(ly.actor, cy.actor) AS actor,
        COALESCE(ly.start_date, cy.current_year) AS start_date,
        COALESCE(ly.end_date, cy.current_year) AS end_date,
        CASE
            WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN TRUE
            WHEN ly.is_active = cy.is_active  AND ly.quality_class = cy.quality_class THEN FALSE
        END AS did_change,
        ly.is_active AS is_active_last_year,
        cy.is_active AS is_active_this_year,
        ly.quality_class AS quality_class_last_year,
        cy.quality_class AS quality_class_this_year,
        2022 AS current_year
    FROM last_year AS ly 
    FULL OUTER JOIN current_year AS cy
        ON ly.actor = cy.actor
        AND ly.end_date + 1 = cy.current_year
),

changed as (

    SELECT
        actor,
        current_year,
        CASE
            WHEN did_change = FALSE THEN ARRAY[
                CAST(
                    ROW(
                        is_active_last_year,
                        quality_class_last_year,
                        start_date,
                        end_date + 1
                ) AS ROW(
                    is_active BOOLEAN,
                    quality_class VARCHAR,
                    start_date INTEGER,
                    end_date INTEGER
                )
            )]
            WHEN did_change = TRUE THEN ARRAY[
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
                )),
                CAST(
                    ROW(
                        is_active_this_year,
                        quality_class_this_year,
                        current_year,
                        current_year
                ) AS ROW(
                    is_active BOOLEAN,
                    quality_class VARCHAR,
                    start_date INTEGER,
                    end_date INTEGER
                )
            )]
            WHEN did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(
                            is_active_last_year,
                            is_active_this_year
                        ),
                        COALESCE(
                            quality_class_last_year, 
                            quality_class_this_year
                        ),
                        start_date,
                        end_date
                ) AS ROW(
                    is_active BOOLEAN,
                    quality_class VARCHAR,
                    start_date INTEGER,
                    end_date INTEGER
                )
            )] END AS change_array
        FROM combined
)

select 
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date as start_date,
    arr.end_date as end_date,
    current_year
from changed
cross join unnest(change_array) as arr
