-- assumption:
--   currently the data loaded till 2002
--   data is being loaded for 2003 as incremental upload
insert into fayiztk.actors_history_scd
WITH
    last_year_scd AS (
        SELECT
            *
        FROM
            fayiztk.actors_history_scd
        WHERE
            current_year = 2002
    ),
    current_year_scd AS (
        select
            actor,
            quality_class,
            is_active,
            current_year
        from
            fayiztk.actors
        where
            current_year = 2003
    ),
    combined AS (
        SELECT
            COALESCE(ly.actor, cy.actor) AS actor,
            COALESCE(ly.start_date, cy.current_year) AS start_date,
            COALESCE(ly.end_date, cy.current_year) AS end_date,
            CASE
                WHEN ly.is_active <> cy.is_active
                or ly.quality_class <> cy.quality_class THEN 1
                WHEN ly.is_active = cy.is_active
                and ly.quality_class = cy.quality_class THEN 0
            END AS did_change,
            ly.is_active AS is_active_last_year,
            cy.is_active AS is_active_current_year,
            ly.quality_class as quality_class_last_year,
            cy.quality_class as quality_class_current_year,
            2003 AS current_year
        FROM
            last_year_scd ly
            FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor
            AND ly.end_date + 1 = cy.current_year
    ),
    changes AS (
        SELECT
            actor,
            CASE
                WHEN did_change = 0 THEN ARRAY[
                    ROW (
                        quality_class_last_year,
                        is_active_last_year,
                        start_date,
                        end_date + 1
                    )
                ]
                WHEN did_change = 1 THEN ARRAY[
                    ROW (
                        quality_class_last_year,
                        is_active_last_year,
                        start_date,
                        end_date
                    ),
                    ROW (
                        quality_class_current_year,
                        is_active_current_year,
                        current_year,
                        current_year
                    )
                ]
                WHEN did_change IS NULL THEN ARRAY[
                    ROW (
                        COALESCE(
                            quality_class_last_year,
                            quality_class_current_year
                        ),
                        COALESCE(is_active_last_year, is_active_current_year),
                        start_date,
                        end_date
                    )
                ]
            END AS change_array
        FROM
            combined
    )
SELECT
    changes.actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    2003 as current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr (quality_class, is_active, start_date, end_date)