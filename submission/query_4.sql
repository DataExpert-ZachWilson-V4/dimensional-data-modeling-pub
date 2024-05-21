INSERT INTO grisreyesrios. actors_history_scd

WITH previous_year AS (
    SELECT
        actor,
        is_active,
        quality_class,
        LAG(is_active) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS is_active_last_year,
        LAG(quality_class) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS quality_class_last_year,
        current_year
    FROM grisreyesrios.actors
    WHERE current_year <= 2021
),

changed AS (
    SELECT
        *,
        --  if is_active OR quality_class has changed
        SUM(
            CASE
                WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor
            ORDER BY
                current_year
        ) AS changed_identifier
    FROM previous_year
)

SELECT
    actor,
    MAX(quality_class) AS quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 AS current_year
FROM changed
GROUP BY actor, changed_identifier
