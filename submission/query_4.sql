INSERT INTO chinmay_hebbal.actors_history_scd
WITH lagged AS (
    SELECT actor,
        actor_id,
        average_rating,
        quality_class,
        CASE
            WHEN is_active THEN 1
            ELSE 0
        END AS is_active,
        CASE
            WHEN LAG (is_active, 1) OVER (
                PARTITION BY actor_id
                ORDER BY current_year ASC
            ) THEN 1
            ELSE 0
        END AS is_active_last_year,
        current_year
    FROM chinmay_hebbal.actors
),
streaked AS (
    SELECT *,
        SUM(
            CASE
                WHEN is_active <> is_active_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS streak_identifier
    FROM lagged
)
SELECT
    actor,
    actor_id,
    CAST(MAX(is_active) AS BOOLEAN) AS is_active,
    COALESCE(AVG(average_rating), 0) AS average_rating,
    COALESCE(MAX(quality_class), 'unknown') AS quality_class,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2012 AS current_year
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    streak_identifier
