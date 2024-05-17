-- Insert data into the actors_history_scd table
INSERT INTO rajkgupta091041107.actors_history_scd
-- Common Table Expressions (CTEs) to prepare data for insertion
WITH 
-- CTE to get the latest actors' records from the history table
last_actors_scd AS (
    SELECT * 
    FROM rajkgupta091041107.actors_history_scd 
    WHERE EXTRACT(YEAR FROM end_date) = (
        SELECT EXTRACT(YEAR FROM MAX(end_date)) FROM rajkgupta091041107.actors_history_scd
    )
),
-- CTE to get the current actors' records
current_actors_scd AS (
    SELECT * 
    FROM rajkgupta091041107.actors
    WHERE current_year = 2021
),
-- CTE to combine the latest and current actors' records
combined AS (
    SELECT
        -- Combine actor_id from both tables
        COALESCE(las.actor_id, cas.actor_id) AS actor_id,
        -- Set start_date as either the latest end_date or the current year's start date
        COALESCE(las.end_date, CAST(CAST(cas.current_year AS VARCHAR) || '-01-01' AS DATE)) AS start_date,
        -- Set end_date as either the latest end_date or the current year's end date
        COALESCE(las.end_date, CAST(CAST(cas.current_year AS VARCHAR) || '-12-31' AS DATE)) AS end_date,
        -- Determine if there's a change in quality_class from last year
        CASE
            WHEN las.quality_class <> cas.quality_class THEN 1
            WHEN las.quality_class = cas.quality_class THEN 0
        END AS quality_did_change,
        -- Determine if there's a change in is_active from last year
        CASE
            WHEN las.is_active <> cas.is_active THEN 1
            WHEN las.is_active = cas.is_active THEN 0
        END AS is_active_did_change,
        -- Record the quality_class from last year
        las.quality_class AS is_quality_class_last_year,
        -- Record the quality_class for the current year
        cas.quality_class AS is_quality_class_this_year,
        -- Record the is_active status from last year
        las.is_active AS is_active_last_year,
        -- Record the is_active status for the current year
        cas.is_active AS is_active_this_year,
        -- Record the modified date
        current_date AS modified_date
    FROM
        last_actors_scd las
        FULL OUTER JOIN current_actors_scd cas ON las.actor_id = cas.actor_id
        AND date_add('year', 1, las.end_date) = date_add('year', 1, date_parse(CAST(cas.current_year AS VARCHAR) || '-01-01', '%Y-%m-%d'))
),
-- CTE to assign change_array based on quality_class and is_active changes
changes AS (
    SELECT
        actor_id,
        modified_date,
        CASE
            WHEN is_active_did_change = 0 OR quality_did_change = 0 THEN ARRAY[ CAST(
                ROW(is_quality_class_last_year, is_active_last_year, start_date, date_add('year', 1, end_date)) AS ROW(
                    quality_class VARCHAR,  
                    is_active BOOLEAN,
                    start_date DATE,
                    end_date DATE
                )
            ) ]
            WHEN is_active_did_change = 1 OR quality_did_change = 1 THEN ARRAY[
                CAST(
                    ROW(is_quality_class_last_year, is_active_last_year, start_date, end_date) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date DATE,
                        end_date DATE
                    )
                ),
                CAST(
                    ROW(is_quality_class_this_year, is_active_this_year, start_date, date_add('year', 1, end_date)) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date DATE,
                        end_date DATE
                    )
                )
            ]
            WHEN quality_did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(is_quality_class_last_year, is_quality_class_this_year),
                        COALESCE(is_active_last_year, is_active_this_year),
                        start_date,
                        end_date
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date DATE,
                        end_date DATE
                    )
                )
            ]
        END AS change_array 
    FROM 
        combined
)
-- Select and format the final results for insertion
SELECT
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    modified_date
FROM
    changes
    CROSS JOIN UNNEST (change_array) AS arr
