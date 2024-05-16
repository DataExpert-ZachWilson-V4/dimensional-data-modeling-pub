-- Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD data with the new incoming data from the actors table for this year.
INSERT INTO actors_history_scd
WITH last_year_scd AS ( -- CTE to hold last year's SCD data
    SELECT
        *
    FROM
        actors_history_scd
    WHERE
        current_year = 2011
),
current_year_scd AS ( -- CTE to hold current year's SCD data
    SELECT
        *
    FROM
        actors
    WHERE
        current_year = 2012
),
combined AS ( -- CTE to combine last year + current year's SCD data
    SELECT
        COALESCE(ly.actor, cy.actor) AS actor,
        ly.quality_class AS quality_class_last_year,
        cy.quality_class AS quality_class_current_year,
        ly.is_active AS is_active_last_year,
        cy.is_active AS is_active_current_year,
        -- Record a change if there's a change in is_active OR quality_class
        CASE
            WHEN ly.is_active <> cy.is_active
            OR ly.quality_class <> cy.quality_class THEN 1
            WHEN ly.is_active = cy.is_active
            AND ly.quality_class = cy.quality_class THEN 0
        END AS did_change,
        COALESCE(ly.start_date, cy.current_year) AS start_date,
        COALESCE(ly.end_date, cy.current_year) AS end_date,
        2012 AS current_year
    FROM
        last_year_scd ly 
        FULL OUTER JOIN current_year_scd cy ON (ly.actor = cy.actor)
        AND (ly.end_date + 1 = cy.current_year)
),
generate_changes AS ( -- CTE to generate the changes that we need to record
    SELECT
        actor,
        current_year,
        -- No change
        CASE
            WHEN did_change = 0 THEN ARRAY [CAST(
        ROW(
            is_active_last_year, quality_class_last_year, start_date, end_date + 1
        ) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER))
    ] -- Did change
            WHEN did_change = 1 THEN ARRAY [CAST(
            ROW(
                is_active_last_year, quality_class_last_year, start_date, end_date
            ) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)),
            CAST(ROW(
                is_active_current_year, quality_class_current_year, current_year, current_year
            ) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER))
        ] -- NULL, new records
            WHEN did_change IS NULL THEN ARRAY [CAST(
            ROW(
                COALESCE(is_active_last_year, is_active_current_year),
                COALESCE(quality_class_last_year, quality_class_current_year),
                start_date,
                end_date
            ) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER))
        ]
        END AS change_array
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
    generate_changes
CROSS JOIN UNNEST (change_array) AS arr