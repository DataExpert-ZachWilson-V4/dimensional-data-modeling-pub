-- "Backfill" query to populate the entire actors_history_scd

INSERT INTO actors_history_scd
WITH lagged AS (
  SELECT
    a.actor,
    a.actor_id,
    a.quality_class,
    CASE WHEN is_active THEN 1 ELSE 0 END as is_active,
    CASE WHEN LAG(is_active, 1) OVER(PARTITION BY a.actor_id ORDER BY a.current_year) THEN 1 ELSE 0 END as is_active_last_year,
    a.current_year
  FROM actors a
),
active_years AS (
  SELECT
    l.*,
    SUM(
      CASE 
        WHEN is_active <> is_active_last_year THEN 1 
        ELSE 0 
      END) OVER(PARTITION BY actor_id ORDER BY current_year) as active_identifier
  FROM lagged l
)
SELECT
  actor,
  actor_id,
  quality_class,
  CASE
    WHEN MAX(is_active) = 1 THEN TRUE
    WHEN MAX(is_active) = 0 THEN FALSE
  END as is_active,
  MIN(current_year) as start_date,
  MAX(current_year) as end_date
FROM active_years
GROUP BY actor_id, actor, quality_class, active_identifier
