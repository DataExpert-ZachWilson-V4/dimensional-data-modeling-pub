-- SCD Batch Backfill, entire table in one query.

-- SHOW CREATE TABLE changtiange199881320.actors_history_scd

INSERT INTO changtiange199881320.actors_history_scd

WITH lagged AS (
    SELECT
        actor, 
        quality_class, 
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active, 
        CASE WHEN LAG(is_active, 1) OVER 
             (PARTITION BY actor ORDER BY current_year) THEN 1 
             ELSE 0 END AS is_active_last_year, 
        current_year
    FROM 
        changtiange199881320.actors
),
streaked AS(
    SELECT 
        *, 
        SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER 
            (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM 
        lagged
)
SELECT 
    actor, 
    quality_class, 
    MAX(is_active) = 1 AS is_active, -- MAX(is_active) = 1show true/false
    MIN(current_year) AS start_date, -- MAX(is_active) only show 1/0
    MAX(current_year) AS end_date, 
    2021 AS current_year
FROM 
    streaked
GROUP BY 
    actor,
    quality_class,
    streak_identifier