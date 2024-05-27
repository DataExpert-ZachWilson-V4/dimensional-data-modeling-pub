INSERT INTO lsleena.actors_history_scd
WITH
    -- This contains SCD data from the previous year
    last_year_scd AS (
        SELECT
            *
        FROM
            lsleena.actors_history_scd
        WHERE
            current_year = 1917
    ),
     -- This contains data from the current year to be loaded
    this_year_scd AS (
        SELECT
            *
        FROM
            lsleena.actors
        WHERE
            current_year = 1918
    ),
    combined AS (
        SELECT
            COALESCE(l.actor_id, t.actor_id) AS actor_id,
            COALESCE(l.start_date, t.current_year) AS start_date,
            COALESCE(l.end_date, t.current_year) AS end_date,
            CASE
                WHEN l.is_active <> t.is_active THEN 1
                WHEN l.is_active = t.is_active THEN 0
            END AS active_did_change,
            l.is_active AS is_active_last_year,
            t.is_active AS is_active_this_year,
            --Check quality_class and is_active to detect a change. A NULL for did_change indicates a previous year which will be carried forward with no changes.
            CASE
                WHEN l.quality_class <> t.quality_class THEN 1
                WHEN l.quality_class = t.quality_class THEN 0
            END AS class_did_change,
            l.quality_class AS quality_class_last_year,
            t.quality_class AS quality_class_this_year,
            1918 AS current_year
        FROM
            last_year_scd AS l
            FULL OUTER JOIN this_year_scd AS t ON t.actor_id = l.actor_id
            AND l.end_date + 1 = t.current_year
    ),
    changes AS (
        SELECT
            actor_id,
            current_year,
            CASE
            -- Group SCD dimension data into an array
            -- Based if the dimension changed, didn't change, or is a new record
                WHEN active_did_change = 0
                AND class_did_change = 0 THEN ARRAY[
                    CAST(
                        ROW(
                            quality_class_last_year,
                            is_active_last_year,
                            start_date,
                            end_date + 1
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
                WHEN active_did_change = 1
                OR class_did_change = 1 THEN ARRAY[
                    CAST(
                        ROW(
                            quality_class_last_year,
                            is_active_last_year,
                            start_date,
                            end_date
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    ),
                    CAST(
                        ROW(
                            quality_class_this_year,
                            is_active_this_year,
                            current_year,
                            current_year
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
                ELSE ARRAY[
                    CAST(
                        ROW(
                            COALESCE(quality_class_last_year, quality_class_this_year),
                            COALESCE(is_active_last_year, is_active_this_year),
                            start_date,
                            end_date
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
            END AS change_array
        FROM
            combined
    )
SELECT
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr