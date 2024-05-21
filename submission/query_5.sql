-- Incremental query that populates one year's actors_history_scd table by combining last year with this year


INSERT INTO shruthishridhar.actors_history_scd
WITH
    last_year_actor_scd AS (    -- get last year actor scd from actors_history_scd table
        SELECT
            actor,
            quality_class,
            is_active,
            current_year AS start_date,
            current_year AS end_date,
            current_year
        FROM shruthishridhar.actors_history_scd
        WHERE current_year = 2015
    ),
    current_year_actor_scd AS ( -- get this year actor data from actors table
        SELECT * FROM shruthishridhar.actors
        WHERE current_year = 2016
    ),
    combined AS (   -- combine data from last year scd and this year and check for changes
        SELECT
            COALESCE(ls.actor, cs.actor) AS actor,
            ls.is_active AS is_active_last_year,    -- is_active status from last year
            cs.is_active AS is_active_this_year,    -- is_active status from this year
            ls.quality_class AS quality_class_last_year,   -- quality_class from last year
            cs.quality_class AS quality_class_this_year,   -- quality_class from this year
            CASE
                WHEN ls.is_active <> cs.is_active AND ls.quality_class <> cs.quality_class THEN 1   -- if both are not same, then 1
                WHEN ls.is_active = cs.is_active AND ls.quality_class = cs.quality_class THEN 0   -- if both are same, then 0
            END AS combined_change_identifier,  -- identify changes in quality_class and is_active for each actor
            COALESCE(ls.start_date, cs.current_year) AS start_date,
            COALESCE(ls.end_date, cs.current_year) AS end_date,
            2016 AS current_year
        FROM last_year_actor_scd ls
        FULL OUTER JOIN current_year_actor_scd cs
        ON ls.actor = cs.actor AND ls.end_date + 1 = cs.current_year
    ),
    changed AS (    -- get all the changes as array from combined scd data
        SELECT
            actor,
            current_year,
            CASE
                WHEN combined_change_identifier = 0 THEN ARRAY[ -- if both are same, use last year scd data
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
                WHEN combined_change_identifier = 1 THEN ARRAY[ -- if both are not same, combine
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
                WHEN combined_change_identifier IS NULL THEN ARRAY[ -- if change is null, coalesce
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
            END AS combined_change_array
        FROM combined
    )
SELECT
    actor,
    unnested_combined_change_array.quality_class,   -- get fields from unnested change array
    unnested_combined_change_array.is_active,
    unnested_combined_change_array.start_date,
    unnested_combined_change_array.end_date,
    current_year
FROM changed
CROSS JOIN UNNEST(combined_change_array) AS unnested_combined_change_array -- Unnest the change array for insertion