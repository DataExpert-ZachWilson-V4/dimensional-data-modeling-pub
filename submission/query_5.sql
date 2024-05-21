INSERT INTO jb19881.actors_history_scd
WITH
    previous_year_scd AS (
        SELECT
            *
        FROM
            jb19881.actors_history_scd
        WHERE
            current_year = 1917 - 1
    ),
    current_year_scd AS (
        SELECT
            *,
            DATE(CAST(current_year as varchar) || '-01-01') as start_date,
            DATE(CAST(current_year as varchar) || '-12-31') as end_date
        FROM
            jb19881.actors
        WHERE
            current_year = 1917 
    ),
    combined AS (
        SELECT
            COALESCE(ls.actor, cs.actor) AS actor,
            COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
            COALESCE(ls.start_date, cs.start_date) AS start_date,
            COALESCE(ls.end_date, cs.end_date) AS end_date,
            ls.is_active <> cs.is_active OR ls.quality_class <> cs.quality_class AS did_change,
            ls.is_active AS is_active_previous_year,
            cs.is_active AS is_active_current_year,
            ls.quality_class AS quality_class_previous_year,
            cs.quality_class AS quality_class_current_year,
            1917 AS current_year
        FROM
            previous_year_scd ls
            FULL OUTER JOIN current_year_scd cs ON ls.actor_id = cs.actor_id
            AND YEAR(ls.end_date) + 1 = cs.current_year
    ),
    changes AS (
        SELECT
            actor,
            actor_id,
            current_year,
            CASE
                WHEN NOT did_change THEN ARRAY[
                    CAST(
                        ROW(is_active_previous_year, quality_class_previous_year, start_date, DATE_ADD('year', 1, end_date + 1) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_date date,
                            end_date date
                        )
                    )
                ]
                WHEN did_change THEN ARRAY[
                    CAST(
                        ROW(is_active_previous_year, quality_class_previous_year, start_date, end_date) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_date date,
                            end_date date
                        )
                    ),
                    CAST(
                        ROW(is_active_current_year, quality_class_current_year, current_year, current_year) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_date date,
                            end_date date
                        )
                    )
                ]
                WHEN did_change IS NULL THEN ARRAY[
                    CAST(
                        ROW(
                            COALESCE(is_active_previous_year, is_active_current_year),
                            COALESCE(quality_class_previous_year, quality_class_current_year),
                            start_date,
                            end_date
                        ) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_date date,
                            end_date date
                        )
                    )
                ]
            END AS change_array
        FROM
            combined
    )
    -- select * from changes

SELECT
    actor,
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr