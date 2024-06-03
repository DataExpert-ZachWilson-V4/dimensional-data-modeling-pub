--test llm
INSERT INTO nattyd.actors_history_scd
WITH lagger AS (
    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        LAG(is_active,1) OVER (
                PARTITION BY actorid
                ORDER BY current_year
        ) AS is_active_last_year,
        LAG(quality_class,1) OVER (
                PARTITION BY actorid
                ORDER BY current_year
        ) AS quality_class_last_year,
        current_year
    FROM
        nattyd.actors
    WHERE current_year <= 1923
),
streaked AS (
    SELECT *,
        SUM( 
            CASE 
                WHEN is_active <> is_active_last_year 
                    THEN 1
                WHEN quality_class <> quality_class_last_year
                    THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actorid
            ORDER BY current_year
        ) AS streak
    FROM lagger
)

SELECT 
    actor,
    actorid,
    quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    1923 AS current_year
FROM streaked
GROUP BY actorid, actor, quality_class, streak
