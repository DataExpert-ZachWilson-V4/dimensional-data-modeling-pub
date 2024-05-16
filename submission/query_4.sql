-- Initialize the actors_history_scd table with a backfill until 2009
INSERT INTO erich.actors_history_scd
WITH lagged AS (
    SELECT
        actor_id,
        actor,
        quality_class,
        LAG(quality_class, 1) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS quality_class_last_year,
        is_active,
        LAG(is_active, 1) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS is_active_last_year,
        current_year
    FROM
        erich.actors
    WHERE current_year <= 2009
),
streaked AS (
    SELECT
        *,
        -- Rolling streak identifier for quality_class
        SUM(
            CASE
                WHEN quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS quality_streak_identifier,
        -- Rolling streak identifier for is_active
        SUM(
            CASE
                WHEN is_active <> is_active_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor_id ORDER BY current_year
        ) AS active_streak_identifier
    FROM
        lagged
)
SELECT
    actor_id,
    actor,
    MAX(quality_class) AS quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2009 AS current_year
FROM
    streaked
GROUP BY
    actor_id,
    actor,
    active_streak_identifier,
    quality_streak_identifier
ORDER BY
    actor_id, current_year, start_date