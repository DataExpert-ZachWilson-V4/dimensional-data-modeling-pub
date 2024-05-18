
INSERT INTO jlcharbneau.actors_history_scd

WITH lagged AS (
    SELECT *,
           LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
            LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
    FROM jlcharbneau.actors
),
streaked AS (
    SELECT *,
        SUM(
            CASE
                WHEN quality_class <> previous_quality_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
                END
        ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM lagged
)

SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    DATE(CONCAT(CAST(MIN(streaked.current_year) AS VARCHAR), '-01-01')) AS start_date,
    COALESCE(
        date_add('day', -1, DATE(CONCAT(CAST(MAX(streaked.current_year) AS VARCHAR), '-01-01'))),
        DATE '9999-12-31'
    ) AS end_date,
    streaked.current_year
FROM streaked
GROUP BY actor, actor_id, quality_class, is_active, streak_identifier, streaked.current_year
ORDER BY actor_id, start_date