-- Insert data into SCD (type 2) table for actors data using a single query to track changes in attributes quality_class and is_active over the time
INSERT INTO raviks90.actors_history_scd
WITH
    -- Derive the previous value for quality_class and is_active flag to compare later
    changes AS (
        SELECT
            actor_id,
            quality_class,
            COALESCE(
                LAG(quality_class) OVER (
                    PARTITION BY
                        actor_id
                    ORDER BY
                        current_year
                ) != quality_class,
                false
            ) AS quality_class_change,
            is_active,
            COALESCE(
                LAG(is_active) OVER (
                    PARTITION BY
                        actor_id
                    ORDER BY
                        current_year
                ) != is_active,
                false
            ) AS is_active_change, 
            current_year
        FROM
            raviks90.actors
        WHERE
            current_year <= 2021 -- Since latest data is upto year 2021
    ),
    streak AS (
        -- Identify and mark the changes to attributes by comparing current year data with previous year
        SELECT
            actor_id,
            quality_class,
            is_active,
            -- when attributes are changed rnk value would increase by 1 else remain unchanged
            SUM(
                CASE
                    WHEN quality_class_change = true 
                    OR is_active_change = true then 1
                    WHEN quality_class_change = false
                    AND is_active_change = false then 0
                END
            ) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS rnk,
            current_year
        FROM
            changes
    )
SELECT
    actor_id,
    MAX(quality_class) AS quality_class, -- max or min doesnt matter since we track every change anyway 
    MAX(is_active) AS is_active,  -- max or min doesnt matter since we track every change anyway
    MIN(current_year) AS start_date, -- min to set the start period 
    MAX(current_year) AS end_date, -- max to mark the end period 
    2021 AS current_year -- represents the latest year;
FROM
    streak
GROUP BY
    actor_id,
    rnk
