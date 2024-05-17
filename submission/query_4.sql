-- ======================================================
INSERT INTO andreskammerath.actors_history_scd
WITH changes AS (
    SELECT
        actor,
        quality_class,
        is_active,
        current_year AS start_date,
        LAG(quality_class) OVER (PARTITION BY actor ORDER BY current_year) AS prev_quality_class,
        LAG(is_active) OVER (PARTITION BY actor ORDER BY current_year) AS prev_is_active
    FROM andreskammerath.actors
),
grouped_changes AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        SUM(
            CASE
                WHEN quality_class = prev_quality_class AND is_active = prev_is_active THEN 0
                ELSE 1
            END
        ) OVER (PARTITION BY actor ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS group_num
    FROM changes
),
final_query AS (
    SELECT
        actor,
        quality_class,
        is_active,
        MIN(start_date) AS start_date,
        MAX(start_date) AS end_date
    FROM grouped_changes
    GROUP BY actor, quality_class, is_active, group_num
    ORDER BY start_date
)
SELECT * FROM final_query
