-- "Backfill" query that can populate the entire actors_history_scd 
-- table in a single query
INSERT INTO positivelyamber.actors_history_scd
-- Get history from actor's table
WITH lagged as (
    SELECT 
        actor,
        actor_id,
        quality_class,
        is_active,
        -- Actor's active status last year 
        LAG(is_active, 1) OVER (PARTITION BY actor_id 
            ORDER BY current_year) as is_active_last_year,
        -- Actor's quality class last year
        LAG(quality_class, 1) OVER (PARTITION BY actor_id 
            ORDER BY current_year) as quality_class_last_year,
        current_year
    FROM positivelyamber.actors
    -- Choosing max current year of 1997 to capture ~5 years of data
    WHERE current_year <= 1997
),
streaked AS (
    SELECT 
        *,
         SUM(
            CASE 
                -- Check if active status or quality class changed from last year
                WHEN is_active <> is_active_last_year 
                OR quality_class <> quality_class_last_year
                THEN 1 ELSE 0 
            END
        )
        OVER(PARTITION BY actor_id ORDER BY current_year) 
            AS streak_identifier
    FROM lagged
)

-- Find start date and end date for the change
SELECT 
    actor,
    actor_id, 
    MAX(is_active) = 1 as is_active, 
    MIN(current_year) as start_date,
    MAX(current_year) as end_date,
    2007 as current_year
FROM streaked
GROUP BY actor_id, quality_class, streak_identifier