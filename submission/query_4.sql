-- Backfill Query that can populate the entire actors_history_scd in a single query
INSERT INTO devpatel18.actors_history_scd
WITH lagged AS (
    SELECT
        *,
        CASE 
         WHEN is_active THEN 1 ELSE 0 
        END AS is_active_flag,
        -- Last year's quality class
        LAG(quality_class, 1) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS quality_class_last_year,
        -- last year's active status
        CASE
            WHEN LAG(is_active, 1) OVER (
                PARTITION BY actor
                ORDER BY current_year
            ) THEN 1 ELSE 0
        END AS is_active_last_year 
    FROM
        devpatel18.actors
),
-- how long actor has been active, with a particular quality class
streaked AS (
    SELECT
        *,
        SUM(
            -- compare active stauts and quality_class for
            CASE
                WHEN is_active_flag <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS streak_identifier
    FROM
        lagged
)

-- grouping by activity, quality class and streak
SELECT 
    actor,
    actor_id,
    quality_class,
    MAX(is_active_flag) AS is_active_flag,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    2014 AS current_year
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    is_active_flag,
    quality_class,
    streak_identifier
