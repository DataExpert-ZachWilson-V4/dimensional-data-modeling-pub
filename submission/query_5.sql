INSERT INTO
    barrocaeric.actors_history_scd
WITH
    last_year_scd AS (
        SELECT
            *
        FROM
            barrocaeric.actors_history_scd
        WHERE
            current_year = 1999
    ),
    current_year_scd AS (
        SELECT
            *
        FROM
            barrocaeric.actors
        WHERE
            current_year = 2000
    ),
    -- Worked with two different columns to identify changes on quality_class and is_active
    combined AS (
        SELECT
            COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
            COALESCE(ls.actor, cs.actor) AS actor,
            COALESCE(ls.start_date, cs.current_year) AS start_date,
            COALESCE(ls.end_date, cs.current_year) AS end_date,
            ls.quality_class as quality_class_last_year,
            cs.quality_class as quality_class_this_year,
            CASE
                WHEN ls.quality_class <> cs.quality_class
                OR ls.is_active <> cs.is_active THEN 1
                WHEN ls.quality_class = cs.quality_class
                AND ls.is_active = cs.is_active THEN 0
            END AS did_change,
            ls.is_active as is_active_last_year,
            cs.is_active as is_active_this_year,
            2000 AS current_year
        FROM
            last_year_scd ls
            FULL OUTER JOIN current_year_scd cs ON ls.actor_id = cs.actor_id
            AND ls.end_date + 1 = cs.current_year
    ),
    -- Here I chose to use only one array to aggregate the values of changes in quality_class
    -- and is_active
    changes AS (
        SELECT
            actor_id,
            actor,
            current_year,
            CASE
                WHEN did_change = 0 THEN ARRAY[
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
                WHEN did_change = 1 THEN ARRAY[
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
                -- If one is NULL the other also has to be
                WHEN did_change IS NULL THEN ARRAY[
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
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) as arr