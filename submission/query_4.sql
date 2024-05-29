INSERT INTO luiscoelho37431.actors_history_scd
WITH active_info_cte AS (
    -- Common Table Expression to retrieve active information for actors
    SELECT
        actor,
        quality_class,
        actor_id,
        is_active,
        -- Retrieve the value of 'is_active' for the previous year using LAG function
        LAG(is_active, 1) OVER (
            PARTITION BY
            actor_id
            ORDER BY
            current_year
        ) AS is_active_last_year,
        current_year
    FROM
        luiscoelho37431.actors
),
-- Common Table Expression to calculate streaks of active/inactive periods for actors
streak_cte AS (
    -- This block uses a window function to assign a streak ID to each row in the active_info_cte CTE.
    -- The streak ID increments whenever there is a change in the 'is_active' column compared to the previous year.
    -- This allows us to group consecutive active/inactive periods together.
    SELECT *,
        SUM(
            CASE
            WHEN is_active <> is_active_last_year THEN 1
            ELSE 0
            END
        ) OVER (
            PARTITION BY
            actor_id
            ORDER BY
            current_year
        ) AS streak_id
    FROM active_info_cte
)
-- Select the actor_id, quality_class, maximum is_active value, minimum current_year value,
-- maximum current_year value, and a fixed value of 2001 as the current_year.
-- The MAX(is_active) will give us the overall is_active status for each actor_id and streak_id combination.
-- The MIN(current_year) will give us the start_date of each streak and MAX(current_year) the end_date.
-- The fixed value of 2001 as the current_year is used to represent the current year in the result set.
SELECT
    actor_id,
    quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2001 AS current_year
FROM
    streak_cte
GROUP BY
    actor_id,
    streak_id,
    quality_class