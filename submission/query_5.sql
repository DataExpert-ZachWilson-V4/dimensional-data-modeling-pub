-- "Incremental" query that can populate a single year's worth of the 
-- actors_history_scd table by combining the previous year's SCD data 
-- with the new incoming data from the actors table for this year.
INSERT INTO positivelyamber.actors_history_scd
-- Get last year's data
with last_year_scd AS (
    SELECT * FROM positivelyamber.actors_history_scd
    WHERE current_year = 1997
),
-- Get this year's data
this_year_scd AS (
    SELECT * FROM positivelyamber.actors
    WHERE current_year = 1998
),
-- Combine the data while checkng for changes in is_active and qulaity_class
combined as (
    SELECT
        COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
        COALESCE(ly.start_date, cy.current_year) AS start_date,
        COALESCE(ly.end_date, cy.current_year) AS end_date,
        CASE 
            WHEN ly.is_active <> cy.is_active 
                OR ly.quality_class <> cy.quality_class THEN 1
            WHEN ly.is_active = cy.is_active 
                OR ly.quality_class = cy.quality_class THEN 0
        END as did_change,
        ly.is_active AS is_active_last_year,
        cy.is_active AS is_active_current_year,
        ly.quality_class AS quality_class_last_year,
        cy.quality_class AS quality_class_current_year,
        1998 AS current_year
    FROM last_year_scd ly
        FULL OUTER JOIN this_year_scd cy
        ON ly.actor_id = cy.actor_id AND ly.end_date + 1 = cy.current_year
),
changes AS (
    SELECT 
        actor_id,
        CASE 
            WHEN did_change = 0 
                THEN ARRAY[
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
                            COALESCE(is_active_last_year, is_active_current_year),
                            COALESCE(quality_class_last_year, quality_class_current_year),
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
        END as change_array,
        current_year
    FROM combined
)

SELECT
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM changes 
    CROSS JOIN UNNEST(change_array) AS arr