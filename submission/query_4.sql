INSERT INTO ivomuk37854.actors_history_scd 
WITH lagged AS (
    SELECT *,
        LAG(quality_class) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS quality_class_last_year,
        LAG(is_active) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS is_active_last_year
    FROM ivomuk37854.actors
),
changed AS (
    SELECT *,
        CASE
            WHEN quality_class <> quality_class_last_year THEN 1
            WHEN is_active <> is_active_last_year THEN 1
            ELSE 0
        END AS changed
    FROM lagged
),
streak AS (
    SELECT *,
        SUM(changed) OVER(
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS streak
    FROM changed
)
SELECT actor,
    actor_id,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    1921 current_year
FROM streak
GROUP BY actor, actor_id, quality_class, is_active, streak

