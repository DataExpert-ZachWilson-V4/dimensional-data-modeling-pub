-- "Incremental" query that can populate a single year's worth of the actors_history_scd 
-- table by combining the previous year's SCD data with the new incoming data 
-- from the actors table for this year
INSERT INTO
    actors_history_scd
WITH
    -- CTE to fetch the data from the 'actors_history_scd' for the specific year (the last,that we had in the table)
    last_year_scd AS (
        SELECT
            *
        FROM
            actors_history_scd
        WHERE
            current_year = 2020
    ),
    -- CTE to fetch next year data from the 'actors' table
    current_year_scd AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 2021
    ),
    -- CTE to combine data from 1917 and 1918 to check for changes
    combined AS (
        SELECT
            COALESCE(ls.actor, cs.actor) as actor,
            COALESCE(ls.quality_class, cs.quality_class) as quality_class,
            COALESCE(ls.start_date, cs.current_year) as start_date,
            COALESCE(ls.end_date, cs.current_year) as end_date,
            -- Detecting a change in the 'is_active' status between the two years
            CASE
                WHEN ls.is_active <> cs.is_active OR ls.quality_class <> cs.quality_class THEN 1
                WHEN ls.is_active = cs.is_active AND ls.quality_class = cs.quality_class THEN 0
            END as did_change,
            ls.is_active as is_active_last_year,
            cs.is_active as is_active_this_year,
            ls.quality_class as quality_class_last_year,
            cs.quality_class as quality_class_this_year,
            2021 as current_year
        FROM
            last_year_scd ls
            FULL OUTER JOIN current_year_scd cs ON ls.actor = cs.actor
            AND ls.end_date + 1 = cs.current_year
    ),
    -- CTE to determining how to record changes based on whether there was a change in 'is_active' status
    changes_recording AS (
        SELECT
            actor,
            current_year,
            -- Constructing an array of records to represent historical periods based on whether there was a change
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
    -- Final SELECT statement that flattens the array from the 'changes' CTE and prepares data for insertion
SELECT
    actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM
    changes_recording
    -- CROSS JOIN with UNNEST is used to expand the 'change_array' into individual rows per historical period
    CROSS JOIN UNNEST (change_array) as arr
