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
            *
        FROM
            jb19881.actors
        WHERE
            current_year = 1917 
    ),
    combined AS (
        SELECT
            COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
            COALESCE(ls.start_year, cs.current_year) AS start_year,
            COALESCE(ls.end_year, cs.current_year) AS end_year,
            ls.is_active <> cs.is_active OR ls.quality_class <> cs.quality_class AS did_change,
            ls.is_active AS is_active_previous_year,
            cs.is_active AS is_active_current_year,
            ls.quality_class AS quality_class_previous_year,
            cs.quality_class AS quality_class_current_year,
            1917 AS current_year
        FROM
            previous_year_scd ls
            FULL OUTER JOIN current_year_scd cs ON ls.actor_id = cs.actor_id
            AND ls.end_year + 1 = cs.current_year
    ),
    changes AS (
        SELECT
            actor_id,
            current_year,
            CASE
                WHEN NOT did_change THEN ARRAY[
                    CAST(
                        ROW(is_active_previous_year, quality_class_previous_year, start_year, end_year + 1) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
                WHEN did_change THEN ARRAY[
                    CAST(
                        ROW(is_active_previous_year, quality_class_previous_year, start_year, end_year) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_year integer,
                            end_year integer
                        )
                    ),
                    CAST(
                        ROW(is_active_current_year, quality_class_current_year, current_year, current_year) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
                WHEN did_change IS NULL THEN ARRAY[
                    CAST(
                        ROW(
                            COALESCE(is_active_previous_year, is_active_current_year),
                            COALESCE(quality_class_previous_year, quality_class_current_year),
                            start_year,
                            end_year
                        ) AS ROW(
                            is_active boolean,
                            quality_class varchar,
                            start_year integer,
                            end_year integer
                        )
                    )
                ]
            END AS change_array
        FROM
            combined
    )
    -- select * from changes

SELECT
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_year,
    arr.end_year,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr