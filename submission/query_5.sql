-- select *  from xeno.actors
-- select actor, quality_class, is_active  from xeno.actors
with
    last_year_scd as (
        select
            *
        from
            actors_history_scd
        where
            end_date = 1917
    ),
    current_year_scd as (
        select
            *
        from
            actors
        where
            current_year = 1918
    ),
    combined as (
        select
            COALESCE(ly.actor, cy.actor) as actor,
            COALESCE(ly.start_date, cy.current_year) as start_year,
            COALESCE(ly.end_date, cy.current_year) as end_year,
            case
                when ly.quality_class <> cy.quality_class OR ly.is_active <> cy.is_active then true
                else false
            end as did_change,
            ly.quality_class AS quality_class_last_year,
            cy.quality_class AS quality_class_current_year,
            ly.is_active as is_active_last_year,
            cy.is_active as is_active_current_year,
            1918 as current_year
        from
            last_year_scd ly
            full outer join current_year_scd cy on ly.actor_id = cy.actor_id
            AND ly.end_date + 1 = cy.current_year
    ),
    changes as (
        select
            actor,
            current_year,
            case
                when did_change = false then array[
                    CAST(
                        ROW(quality_class_last_year, is_active_last_year, start_year, end_year + 1) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
                when did_change = true then array[
                    CAST(
                        ROW(quality_class_last_year,is_active_last_year, start_year, end_year + 1) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_year integer,
                            end_year integer
                        )
                    ),
                    CAST(
                        ROW(
                            quality_class_current_year,
                            is_active_current_year,
                            current_year,
                            current_year
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
                when did_change is null then array[
                    CAST(
                        ROW(
                            COALESCE(quality_class_last_year, quality_class_current_year),
                            COALESCE(is_active_last_year, is_active_current_year),
                            start_year,
                            end_year
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
            END AS change_array
        from
            combined
    )
SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_year,
    arr.end_year,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr