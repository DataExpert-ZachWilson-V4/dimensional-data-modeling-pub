INSERT INTO actors_history_scd

WITH lagged AS (
    -- Fetch actor-related data and compute lagged values for 'quality_class' and 'is_active' status
    SELECT *,
           LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
            LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
    FROM actors
),
     streaked AS (
         -- Calculate a 'streak_identifier' for each actor
         SELECT *,
                SUM(
                        CASE
                            WHEN quality_class <> previous_quality_class THEN 1
                            WHEN is_active <> previous_is_active THEN 1
                            ELSE 0
                            END
                    ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
         FROM lagged
     )

SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    MIN(streaked.current_year) AS start_date,
    MAX(streaked.current_year) AS end_date,
    2021 AS current_year
FROM streaked
GROUP BY actor, actor_id, quality_class, is_active, streak_identifier