WITH lagged AS (
    SELECT 
        actor, actor_id, 
        quality_class, current_year,
        CASE 
            WHEN is_active 
            THEN 1 ELSE 0 END AS is_active,
        CASE 
            WHEN LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) 
            THEN 1 ELSE 0 END AS is_active_last_year
    FROM 
        changtiange199881320.actors
    WHERE 
        current_year <= 2021
),
streaked AS(
    SELECT 
        *, 
        SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) 
        OVER(PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM 
        lagged
)
SELECT 
    actor, actor_id, quality_class, 
    MAX(is_active) = 1 AS is_active, 
    MIN(current_year) AS start_season, 
    MAX(current_year) AS end_season, 
    2021 AS current_year
FROM 
    streaked
GROUP BY 
    actor,
    actor_id, 
    quality_class,
    streak_identifier