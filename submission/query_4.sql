INSERT INTO ChrisTaulbee.actors_history_scd
WITH lagged AS (
SELECT
  actor,
  actor_id,
  is_active,
  quality_class,
  LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as quality_class_last_year,
  LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as is_active_last_year,
  current_year
FROM ChrisTaulbee.actors )
, streaked as (
SELECT
  *,
  SUM(
    CASE 
      WHEN quality_class <> quality_class_last_year THEN 1
      WHEN is_active <> is_active_last_year THEN 1
      ELSE 0
      END) 
    OVER(PARTITION BY actor_id ORDER BY current_year) as streak_identifier,
  MAX(current_year) OVER() as latest_year
FROM lagged )
SELECT
  actor,
  actor_id,
  MAX(quality_class) as quality_class,
  MAX(is_active) as is_active,
  latest_year,
  DATE(CONCAT(CAST(MIN(current_year) as VARCHAR), '-01-01')) as start_date,
  DATE(CONCAT(CAST(MAX(current_year) as VARCHAR), '-12-31')) as end_date
FROM
  streaked
GROUP BY
    actor,
    actor_id,
    streak_identifier,
    latest_year