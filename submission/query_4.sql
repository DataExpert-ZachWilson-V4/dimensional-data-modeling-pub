-- This query inserts data into the table "akp389.actors_history_scd" using a WITH clause for better readability and organization.

-- The first common table expression (CTE) "previous_year_activity" retrieves the previous year's activity of actors from the table "akp389.actors_2" for years up to 2001.
-- It includes columns for actor name, actor ID, quality class, activity status, lagged activity status, and the current year.
-- The LAG() function is used to fetch the previous year's activity status for each actor and actor ID combination.
INSERT INTO akp389.actors_history_scd
WITH previous_year_activity AS (
    SELECT 
        actor,
        actor_id,
        quality_class,
        is_active,
        LAG(is_active,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS p_is_active,
        LAG(quality_class,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS pr_quality_class,
        current_year 
    FROM 
        akp389.actors
    WHERE 
        current_year <= 2000
),

-- The second CTE "streaked" calculates streaks of activity for each actor based on changes in activity status.
-- It includes columns for actor name, actor ID, quality class, current year, activity status, and a streak identifier.
-- The SUM() function with a CASE statement is used to determine streak changes (1 for change, 0 for no change) over partitions of actors ordered by year.

streaked AS (
    SELECT 
        actor,
        actor_id,
        quality_class,
        current_year,
        is_active,
        SUM(CASE WHEN is_active = p_is_active AND quality_class = pr_quality_class THEN 0 ELSE 1 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM 
        previous_year_activity
)

-- The final SELECT statement aggregates the streaked data to determine start and end dates of activity streaks.
-- It selects actor name, actor ID, quality class, activity status, start date, end date, and sets the current year as 2001.
-- The data is grouped by actor, actor ID, activity status, quality class, and streak identifier.

SELECT 
    actor,
    actor_id,
    quality_class,
    is_active,
    MIN(s.current_year) AS start_date,
    MAX(s.current_year) AS end_date,
    2000 AS current_year
FROM 
    streaked s
GROUP BY 
    actor,
    actor_id,
    is_active,
    quality_class,
    streak_identifier