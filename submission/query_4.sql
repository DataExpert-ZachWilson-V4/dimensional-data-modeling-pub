/*
Actors History SCD Table Batch Backfill Query (query_4)
Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
*/

INSERT INTO actors_history_scd

-- step one, we want something that indicates a change in status
-- we want to track changes in quality_class or is_active
WITH lagged AS (
    SELECT
        actor,
        actor_id,
        quality_class,
        LAG(quality_class, 1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) as last_years_quality_class,
        is_active,
        CASE 
            WHEN LAG(is_active, 1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) THEN TRUE
            ELSE FALSE END AS is_active_last_year,
        current_year
    FROM actors
    WHERE current_year <= 1956
),

-- step two, we add a rolling sum that will segment distinct periods of an actor's status
streaked AS (
    SELECT
        *,
        SUM(CASE WHEN (is_active <> is_active_last_year) 
                    OR (quality_class <> last_years_quality_class) 
                 THEN 1 ELSE 0 END)
            OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS streak_id
    FROM lagged
)

-- step three, we summarize the statistics for each streak
SELECT
  actor,
  actor_id, 
  quality_class,
  is_active,
  MIN(current_year) as start_year,
  MAX(current_year) as end_year,
  1956 as current_year
FROM streaked
GROUP BY actor, actor_id, quality_class, is_active, streak_id