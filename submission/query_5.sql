INSERT INTO
    actors_history_scd WITH last_year_scd as (
        SELECT
            *
        FROM
            actors_history_scd
        WHERE
            current_year = 2019
    ),
    current_year_scd as (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 2020
    ),
    combined as (
        SELECT
            COALESCE(ly.actor, cy.actor) as actor,
            ly.quality_class as quality_class_last_year,
            cy.quality_class as quality_class_this_year,
            CASE
                WHEN ly.quality_class <> cy.quality_class
                OR ly.is_active <> cy.is_active THEN 1
                WHEN ly.quality_class = cy.quality_class
                OR ly.is_active = cy.is_active THEN 0
            END as did_change,
            COALESCE(ly.start_date, cy.current_year) as start_date,
            COALESCE(ly.end_date, cy.current_year) as end_date,
            ly.is_active as is_active_last_year,
            cy.is_active as is_active_this_year,
            2020 as current_year
        FROM
            last_year_scd ly FULL
            OUTER JOIN current_year_scd cy ON ly.actor = cy.actor
            AND ly.end_date + 1 = cy.current_year
    ),
    changes AS (
        SELECT
            actor,
            CASE
                WHEN did_change = 0 THEN ARRAY [CAST(
                ROW(quality_class_last_year, is_active_last_year, start_date, end_date + 1)
                 AS ROW (quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
                WHEN did_change = 1 THEN ARRAY [
                CAST(ROW(quality_class_last_year, is_active_last_year, start_date, end_date) AS ROW (quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
                CAST(ROW(quality_class_this_year, is_active_this_year, current_year, current_year) AS ROW (quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
                WHEN did_change IS NULL THEN ARRAY [CAST(ROW(COALESCE(quality_class_this_year, quality_class_last_year), COALESCE(is_active_this_year, is_active_last_year), start_date, end_date) AS 
            ROW (quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
            END AS change_array,
            current_year
        FROM
            combined
    )
SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr
    