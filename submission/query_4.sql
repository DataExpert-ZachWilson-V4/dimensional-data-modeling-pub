INSERT INTO actors_history_scd
WITH lagged AS (
-- CTE tracking changes vs previous year for is_active and quality_class fields
    SELECT actor,
        is_active,
        quality_class,
        LAG(is_active, 1, FALSE) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS is_active_last_year,
        LAG(quality_class, 1) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) as quality_class_last_year,
        current_year
    FROM actors
),
streaked AS (
-- CTE tracking 'streaks' of unchanged dimensions (when both is_active and quality_class stay the same across years)
    SELECT *,
        SUM(
            CASE
                WHEN is_active <> is_active_last_year THEN 1
                WHEN quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS streak_identifier
    FROM lagged
)
SELECT actor,
    quality_class,
    is_active,
    min(current_year) as start_date,
    max(current_year) as end_date,
    1924 as curent_year
FROM streaked
GROUP BY actor,
    is_active,
    streak_identifier,
    quality_class