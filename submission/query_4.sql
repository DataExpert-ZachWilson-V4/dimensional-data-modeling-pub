INSERT INTO dennisgera.actors_history_scd 

WITH 

lagged AS (
    SELECT
        actor,
        is_active,
        quality_class,
        -- LAG function to get the previous year's values
        LAG(is_active) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS is_active_last_year,
        LAG(quality_class) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS quality_class_last_year,
        current_year
    FROM dennisgera.actors
    WHERE current_year <= 2021
),

streaked AS (
    SELECT
        *,
        -- identifier if is_active OR quality_class has changed
        SUM(
            CASE
                WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor
            ORDER BY
                current_year
        ) AS streak_identifier
    FROM lagged
)

SELECT
    actor,
    MAX(quality_class) AS quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 AS current_year
FROM streaked
GROUP BY actor, streak_identifier
