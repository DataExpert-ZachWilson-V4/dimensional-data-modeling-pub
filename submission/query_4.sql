/* Actors History SCD Table Batch Backfill Query (query_4)

Write a "backfill" query that can populate the entire actors_history_scd table in a single query 
*/
INSERT INTO danieldavid.actors_history_scd
-- 1) Lag: pull values of quality_class and is_active from LY
WITH lagged AS (
    SELECT
        actor,
        actor_id,
        quality_class,
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_ly,
        is_active,
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_ly,
        current_year
    FROM danieldavid.actors
),
-- 2) Streak: sum consecutive changes in quality_class or is_active vs LY for each actor
streaked AS (
    SELECT *,
        SUM(CASE WHEN quality_class <> quality_class_ly OR is_active <> is_active_ly THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM lagged
)
-- 3) Backfill: fill scd table using streak_identifier to split the group by based on changes
SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    MIN(st.current_year) AS start_date,
    MAX(st.current_year) AS end_date,
    MAX(cy.current_year) AS current_year
FROM streaked st
-- cannot group by current_year due to aggregate, so use a cross join to get the max year
CROSS JOIN (SELECT MAX(current_year) AS current_year FROM danieldavid.actors) cy
GROUP BY actor, actor_id, quality_class, is_active, streak_identifier