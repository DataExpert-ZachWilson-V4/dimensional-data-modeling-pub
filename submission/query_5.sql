INSERT INTO nattyd.actors_history_scd
WITH last_year_scd AS (
    SELECT * FROM nattyd.actors_history_scd
    WHERE current_year = 1923
),
current_year_scd AS (
    SELECT * FROM nattyd.actors
    WHERE current_year = 1924
),
combined AS (
  
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actorid, cy.actorid) AS actorid,
    ly.quality_class AS quality_class_last_year, 
    cy.quality_class AS quality_class_this_year,
    COALESCE(ly.start_date, cy.current_year) AS start_date,
    COALESCE(ly.end_date, cy.current_year) AS end_date,
    CASE 
        WHEN (
        ly.is_active <> cy.is_active 
            OR ly.quality_class <> cy.quality_class
        ) THEN 1
        WHEN ly.is_active = cy.is_active THEN 0
    END AS did_change,
    ly.is_active AS is_active_last_year,
    cy.is_active AS is_active_this_year,

    1924 AS current_year
  FROM
    last_year_scd ly
    FULL OUTER JOIN current_year_scd cy ON 
        ly.actorid = cy.actorid
        AND ly.end_date + 1 = cy.current_year
),
changes AS (
    SELECT
        actor,
        actorid,
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
                    )
                ]
            WHEN did_change IS NULL 
                THEN ARRAY[
                    CAST(
                        ROW(
                            COALESCE(is_active_last_year, is_active_this_year),
                            COALESCE(quality_class_last_year, quality_class_this_year),
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
        END AS change_array,
        current_year
    FROM combined
)

SELECT 
    actor,
    actorid,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM 
    changes
    CROSS JOIN UNNEST (change_array) AS arr