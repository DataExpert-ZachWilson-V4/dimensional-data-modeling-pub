-- Insert data into the saismail.actors_history_scd table
INSERT INTO saismail.actors_history_scd
-- Common Table Expressions (CTEs) to define temporary datasets

-- Select actors' last SCD records based on the end date
WITH last_actors_scd AS (
    SELECT
        *
    FROM
        saismail.actors_history_scd
    WHERE
        end_date = (SELECT MAX(end_date) FROM saismail.actors_history_scd)
),
-- Select current actors' data
current_actors_scd AS (
    SELECT
        *
    FROM
        saismail.actors
    WHERE
        "current_year" = (SELECT MAX(end_date) FROM saismail.actors_history_scd) + 1
),
-- Combine the last year's and current year's data for actors
combined AS (
    SELECT
        COALESCE(las.actor_id, cas.actor_id) AS actor_id, -- Use the actor_id from either last year or this year
        COALESCE(las.start_date, cas.current_year) AS start_date, -- Use the start_date from last year or the current year
        COALESCE(las.end_date, cas.current_year) AS end_date, -- Use the end_date from last year or the current year
        CASE
            WHEN las.quality_class <> cas.quality_class THEN 1 -- Mark as changed if quality class differs
            WHEN las.quality_class = cas.quality_class THEN 0 -- Mark as not changed if quality class is the same
        END AS quality_did_change,
        CASE
            WHEN las.is_active <> cas.is_active THEN 1 -- Mark as changed if active status differs
            WHEN las.is_active = cas.is_active THEN 0 -- Mark as not changed if active status is the same
        END AS is_active_did_change,
        las.quality_class AS is_quality_class_last_year, -- Quality class from last year
        cas.quality_class AS is_quality_class_this_year, -- Quality class from this year
        las.is_active AS is_active_last_year, -- Active status from last year
        cas.is_active AS is_active_this_year, -- Active status from this year
        CURRENT_DATE AS "current_date" -- Current date for logging the record
    FROM
        last_actors_scd las
        FULL OUTER JOIN current_actors_scd cas ON las.actor_id = cas.actor_id
        AND las.end_date = cas.current_year
),
-- Determine the changes in quality class and activity status for each actor
changes AS (
    SELECT
        actor_id,
        "current_date",
        CASE
            WHEN is_active_did_change = 0 OR quality_did_change = 0 THEN ARRAY[
                CAST(
                    ROW(
                        is_quality_class_last_year,
                        is_active_last_year,
                        start_date,
                        end_date
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ] -- If no changes, record a single entry with the same quality class and active status
            WHEN is_active_did_change = 1 OR quality_did_change = 1 THEN ARRAY[
                CAST(
                    ROW(
                        is_quality_class_last_year,
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
                        is_quality_class_this_year,
                        is_active_this_year,
                        start_date,
                        end_date
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ] -- If changes detected, record two entries: one with old values, one with new values
            WHEN quality_did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(
                            is_quality_class_last_year,
                            is_quality_class_this_year
                        ),
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
            ] -- If quality change is null, record the current values
        END AS change_array
    FROM
        combined
),
-- Select the final data for insertion into the history table
final_data AS (
    SELECT
        actor_id,
        arr.quality_class,
        arr.is_active,
        arr.start_date,
        arr.end_date,
        "current_date"
    FROM
        changes
        CROSS JOIN UNNEST(change_array) AS arr -- Expand the array into individual rows
)
-- Insert the selected data into the history table
SELECT
    actor_id,
    quality_class,
    is_active,
    start_date,
    end_date,
    EXTRACT(YEAR FROM current_date) as "current_date"
FROM
    final_data
