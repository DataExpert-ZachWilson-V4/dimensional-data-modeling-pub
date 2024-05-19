-- Populate the entire actors_history_scd table using a backfill query
INSERT INTO alissabdeltoro.actors_history_scd
-- Common Table Expression (CTE) to retrieve lagged data for each actor
WITH actor_lagged_data AS (
    SELECT 
        actor_id,
        actor_name,
        is_active,
        current_year,
        -- Retrieve the previous year's is_active value for each actor
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_previous_year
    FROM alissabdeltoro.actors
),
-- Common Table Expression (CTE) to calculate streaks of consecutive years with the same is_active value
actor_streaks AS (
    SELECT 
        *,
        -- Generate a streak identifier based on changes in is_active values
        SUM(CASE WHEN is_active <> is_active_previous_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM actor_lagged_data
)
-- Main query to determine the start and end dates of each streak for each actor
SELECT 
    actor_id,
    actor_name,
    MAX(is_active) AS is_active,  -- Determine the overall is_active value for each actor
    MIN(current_year) AS start_date,  -- Determine the start date of the streak
    MAX(current_year) AS end_date,  -- Determine the end date of the streak
    2021 AS current_year  -- Set the current year for the backfill query
FROM actor_streaks
GROUP BY actor_id, actor_name, streak_identifier  -- Group results by actor_id, actor_name, and streak_identifier
