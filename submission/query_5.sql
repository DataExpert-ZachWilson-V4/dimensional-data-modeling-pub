-- Populate a single year's worth of the actors_history_scd table incrementally
INSERT INTO actors_history_scd (
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
)
-- Common Table Expression (CTE) to fetch data for the previous year
WITH previous_year_scd AS (
    SELECT
        actor_id,
        actor_name,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM
        actors_history_scd
    WHERE
        current_year = 2018 -- Fetch data for the previous year
),

-- Common Table Expression (CTE) to fetch data for the current year
this_year_scd AS (
    SELECT
        actor_id,
        actor AS actor_name,
        quality_class,
        is_active,
        current_year -- Fetch data for the current year
    FROM
        actors
    WHERE
        current_year = 2019
),

-- Common Table Expression (CTE) to combine data from the previous year with the current year
combined_data AS (
    SELECT
        COALESCE(py.actor_id, ty.actor_id) AS actor_id,
        COALESCE(py.actor_name, ty.actor_name) AS actor_name,
        py.quality_class AS quality_class_last_year,
        ty.quality_class AS quality_class_this_year,
        COALESCE(EXTRACT(YEAR FROM py.start_date), ty.current_year) AS start_date,
        COALESCE(EXTRACT(YEAR FROM py.end_date), ty.current_year) AS end_date,
        py.is_active AS is_active_last_year,
        ty.is_active AS is_active_this_year,
        CASE
            WHEN py.is_active IS DISTINCT FROM ty.is_active
                OR py.quality_class IS DISTINCT FROM ty.quality_class THEN 1
            ELSE 0
        END AS did_change,
        ty.current_year
    FROM
        previous_year_scd py
        FULL OUTER JOIN this_year_scd ty ON py.actor_id = ty.actor_id
),

-- Common Table Expression (CTE) to construct arrays representing changes in is_active status
changes AS (
    SELECT
        actor_id,
        actor_name,
        current_year,
        CASE
            -- No change in status, so only include the existing record and update the end date to this year
            WHEN did_change = 0 THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(is_active_last_year, is_active_this_year),
                        COALESCE(quality_class_last_year, quality_class_this_year),
                        start_date,
                        current_year
                    ) AS ROW(
                        is_active BOOLEAN,
                        quality_class VARCHAR,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ]
            -- New actor in the current year, so only include the record for this year
            WHEN did_change = 1
                AND is_active_last_year IS NULL THEN ARRAY[
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
            -- Actor existed last year, but has changes this year
            WHEN did_change = 1
                AND is_active_last_year IS NOT NULL THEN ARRAY[
                CAST(
                    ROW(
                        is_active_last_year,
                        quality_class_last_year,
                        start_date,
                        current_year - 1
                    ) AS ROW(
                        is_active BOOLEAN,
                        quality_class VARCHAR,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                ),
                -- Include the record for the current year with inactive status
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
        END AS change_array
    FROM
        combined_data
)

-- Main query to unnest the change_array and select individual records
SELECT
    actor_id,
    actor_name,
    change_array.quality_class,
    change_array.is_active,
    CAST(CAST(change_array.start_date AS VARCHAR) || '-01-01' AS DATE) AS start_date,
    CAST(CAST(change_array.end_date AS VARCHAR) || '-12-31' AS DATE) AS end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST(changes.change_array) AS change_array
