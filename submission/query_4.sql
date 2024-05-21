INSERT INTO srik1981.actors_history_scd 
WITH last_year AS ( -- CTE to extract the changing dimensions tracked for current and previous year
    SELECT
        actor,
        actor_id,
        quality_class,
        lag(quality_class) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS prev_quality_class,
        is_active,
        lag(is_active) OVER (
            PARTITION BY actor
            ORDER BY current_year
        ) AS prev_is_active,
        current_year
    FROM srik1981.actors
),
result AS ( -- CTE to track if anything changed between previous and current year
    SELECT
        actor,
        actor_id,
        quality_class,
        prev_quality_class,
        is_active,
        prev_is_active,
        SUM(
            CASE
                WHEN quality_class <> prev_quality_class
                OR is_active <> prev_is_active THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor
            ORDER BY
                current_year
        ) AS did_change,
        current_year
    FROM last_year
)
-- Build the final query to be loaded into SCD table
SELECT
    DISTINCT actor,
    actor_id,
    quality_class,
    is_active,
    min(current_year) OVER (PARTITION BY actor, did_change) AS start_date,
    max(current_year) OVER (PARTITION BY actor, did_change) AS end_date,
    max(current_year) OVER () AS current_year
FROM result