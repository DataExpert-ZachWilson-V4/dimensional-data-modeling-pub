INSERT INTO actors_history_scd
WITH
    --incremental data loading with previous data from 1917
    last_year_scd AS (
        SELECT
            *
        FROM
            actors_history_scd
        WHERE
            current_year = 1917
    ),
    --incremental data loading with new data from 1918
    this_year_scd AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 1918
    ),
    --indicators for if is_active status or quality_class changed
    combined AS (
        SELECT
            COALESCE(l.actor, t.actor) AS actor,
            COALESCE(l.start_date, t.current_year) AS start_date,
            COALESCE(l.end_date, t.current_year) AS end_date,
            CASE
                WHEN l.is_active <> t.is_active THEN 1
                WHEN l.is_active = t.is_active THEN 0
            END AS active_did_change,
            l.is_active AS is_active_last_year,
            t.is_active AS is_active_this_year,
            CASE
                WHEN l.quality_class <> t.quality_class THEN 1
                WHEN l.quality_class = t.quality_class THEN 0
            END AS class_did_change,
            l.quality_class AS quality_class_last_year,
            t.quality_class AS quality_class_this_year,
            1918 AS current_year
        FROM
            last_year_scd AS l
            FULL OUTER JOIN this_year_scd AS t ON t.actor = l.actor
            AND l.end_date + 1 = t.current_year
    ),
    changes AS (
        SELECT
            actor,
            current_year,
            CASE
                --if no changes to is_active status or quality_class then increment end_date
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
                --if is_active or quality_class changes add end previous streak and start new one
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
                --if change fields are NULL then this is an already completed record
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
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr