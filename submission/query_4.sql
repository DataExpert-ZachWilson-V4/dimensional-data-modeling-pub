INSERT INTO actors_history_scd (actor_id, actor_name, quality_class, is_active, start_date, end_date, current_year)
-- Common Table Expression (CTE) to retrieve lagged data for each actor
WITH actor_lagged_data AS (
    SELECT 
        actor_id,
        actor AS actor_name,  -- Renamed to match the column name in the table
        quality_class,
        is_active,
        current_year,
        -- Retrieve the previous year's is_active value for each actor
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_previous_year,
        -- Retrieve the previous year's quality_class value for each actor
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_previous_year
    FROM actors
),
-- Common Table Expression (CTE) to calculate streaks of consecutive years with the same is_active value or quality_class
actor_streaks AS (
    SELECT 
        *,
        -- Generate a streak identifier based on changes in is_active values or quality_class
        SUM(CASE WHEN is_active <> is_active_previous_year OR quality_class <> quality_class_previous_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM actor_lagged_data
)
-- Main query to determine the start and end dates of each streak for each actor
SELECT 
    actor_id,
    actor_name,
    MIN(quality_class) AS quality_class,  -- Using MIN to select any value for the streak
    MAX(is_active) AS is_active,  -- Determine the overall is_active value for each actor in the streak
    CAST(MIN(CONCAT(CAST(current_year AS VARCHAR), '-01-01')) AS DATE) AS start_date,  -- Start date set to January 1st of the year
    CAST(MAX(CONCAT(CAST(current_year AS VARCHAR), '-12-31')) AS DATE) AS end_date,  -- End date set to December 31st of the year
    2021 AS current_year  -- Set the current year for the backfill query
FROM actor_streaks
GROUP BY actor_id, actor_name, streak_identifier  -- Group results by actor_id, actor_name, and streak_identifier
