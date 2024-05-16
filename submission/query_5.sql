-- Query to populate the actors history SCD table data one year at a time
INSERT INTO
    pratzo.actors_history_scd
WITH
    -- CTE to get actory history up to the given year 
    last_year_scd AS (
        SELECT *
        FROM pratzo.actors_history_scd
        WHERE
            current_year = 1978
    ),
    -- CTE to get actor data for the next year 
    current_year_scd AS (
        SELECT *
        FROM pratzo.actors
        WHERE
            current_year = 1979
    ),
    -- CTE to select data and combine them to insert into the actor history table
    combined AS (
        SELECT
            COALESCE(ls.actor, cs.actor) as actor,
            COALESCE(ls.quality_class, cs.quality_class) as quality_class,
            COALESCE(ls.start_date, cs.current_year) as start_date,
            COALESCE(ls.end_date, cs.current_year) as end_date,
            ls.is_active as is_active_last_year,
            cs.is_active as is_active_this_year,
            ls.quality_class as quality_class_last_year,
            cs.quality_class as quality_class_this_year,
            -- Tracking change based on whether one of 'quality_class' or 'is_active' changed in the previous year
            CASE
                WHEN ls.quality_class = cs.quality_class
                    AND ls.is_active = cs.is_active THEN 0
                WHEN ls.quality_class <> cs.quality_class
                    OR ls.is_active <> cs.is_active THEN 1
                ELSE NULL
            END AS did_change,
            1979 as current_year
        FROM
            last_year_scd ls
            FULL OUTER JOIN current_year_scd cs ON ls.actor = cs.actor
            AND ls.end_date + 1 = cs.current_year
    ),
    -- CTE to configure how to update data given a change occurred based on 'did_change'
    changes AS (
        SELECT
            actor,
            current_year,
            -- Updating data with changes are re-formatting as an array of structs as needed
            CASE
                WHEN did_change = 0 THEN ARRAY[
                    CAST(
                        ROW(quality_class_last_year, is_active_last_year, start_date, end_date + 1) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                        )
                    )
                ]
                WHEN did_change = 1 THEN ARRAY[
                    CAST(
                        ROW(quality_class_last_year, is_active_last_year, start_date, end_date) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                        )
                    ),
                    CAST(
                        ROW(quality_class_this_year, is_active_this_year, current_year, current_year) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                        )
                    )
                ]
                WHEN did_change IS NULL THEN ARRAY[
                    CAST(
                        ROW(
                            COALESCE(quality_class_last_year, quality_class_this_year),
                            COALESCE(is_active_last_year, is_active_this_year),
                            start_date,
                            end_date
                        ) AS ROW(
                            quality_class VARCHAR,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                        )
                    )
                ]
            END as change_array
        FROM
            combined
    )
SELECT
    actor,
    arr.*,
    current_year
FROM
    changes
    -- CROSS JOIN with UNNEST is used to expand the 'change_array' into individual rows per historical period
    CROSS JOIN UNNEST (change_array) as arr