/*
Actors History SCD Table Batch Backfill Query (query_4)

Write a "backfill" query that can populate the entire actors_history_scd table in a single qu
*/

INSERT INTO harathi.actors_history_scd 
WITH actors_lagged AS (
  SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    LAG(is_active, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS is_active_last_year,
    LAG(quality_class, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS quality_class_last_year,
    current_year
  FROM harathi.actors
  WHERE current_year <= 2021 -- Adjusted to include data up to 2021
),
streaked AS (
  SELECT
    *,
    SUM(
      CASE
        WHEN is_active = is_active_last_year AND quality_class = quality_class_last_year THEN 0
        ELSE 1
      END
    ) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS streak_identifier -- This identifier increments when there is a change in 'is_active' or 'quality_class'
  FROM actors_lagged
)
SELECT
  actor_id,
  actor,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year -- Adjusted to 2021
FROM streaked
GROUP BY actor_id, actor, streak_identifier
