INSERT INTO actors_history_scd
WITH
    --get current and most recent is_active status and quality_class
    lagged AS (
        SELECT
            actor,
            quality_class,
            is_active,
            LAG(is_active, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS is_active_last_year,
            LAG(quality_class, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS quality_class_last_year,
            current_year
        FROM
            actors
    ),
    --a streak means the is_active status and quality class is the same
    streaked AS (
        SELECT
            *,
            SUM(
                CASE
                    WHEN (is_active <> is_active_last_year) OR (quality_class <> quality_class_last_year) THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS streak_identifier
        FROM
            lagged
    )
SELECT
    actor,
    MAX(quality_class) AS quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2001 AS current_year
FROM
    streaked
GROUP BY
    actor,
    streak_identifier