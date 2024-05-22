INSERT INTO human.actors_history_scd (actor, actor_id, quality_class, is_active, current_year, start_date, end_date)

WITH lagged AS (
    -- Fetch actor-related data and compute lagged values for 'quality_class' and 'is_active' status
    SELECT *,
           LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
           LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
    FROM human.actors
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
),
cy AS (
    SELECT MAX(current_year) as max_current_year
    FROM human.actors
)
SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    MAX(cy.max_current_year) AS current_year,
    DATE(CONCAT(CAST(MIN(current_year) AS VARCHAR), '-01-01')) AS start_date,
    DATE(CONCAT(CAST(MAX(current_year) AS VARCHAR), '-12-31')) AS end_date
FROM streaked
CROSS JOIN cy
GROUP BY actor, actor_id, quality_class, is_active, streak_identifier