INSERT INTO jesusr93.actors_history_scd
(
    actor_id,
    quality_class,
    is_active,
    start_year,
    end_year
)
WITH lagged AS (
    SELECT
        *,
        LAG(is_active, 1) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS is_active_last_year,
        LAG(quality_class, 1) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS quality_class_last_year
    FROM jesusr93.actors
    ORDER BY actor_id, current_year
),

streaked AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN is_active <> COALESCE(is_active_last_year, FALSE) THEN 1
                ELSE 0
            END)
            OVER (PARTITION BY actor_id ORDER BY current_year)
        AS streak_active,
        SUM(
            CASE
                WHEN quality_class <> COALESCE(quality_class_last_year, '')
                    THEN 1
                ELSE 0
            END)
            OVER (PARTITION BY actor_id ORDER BY current_year)
        AS streak_qc
    FROM lagged
)

SELECT
    actor_id,
    MAX(quality_class) AS quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year
FROM streaked
GROUP BY actor_id, streak_active, streak_qc
ORDER BY actor_id, start_year
