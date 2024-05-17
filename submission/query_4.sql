
INSERT INTO faraanakmirzaei15025.actors_history_scd
--calculates the previous year (prev_year) and the active status in the last year (is_active_last_year) for each actor
WITH
  actors_lag AS (
    SELECT
      actor,
      actor_id,
      films,
      current_year,
      LAG(current_year) OVER (PARTITION BY actor_id ORDER BY current_year) AS prev_year,
      LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
      is_active,
      quality_class
    FROM
      faraanakmirzaei15025.actors
    WHERE
      current_year < 2024
  ),
  --determines if there was a status change (did_change) in is_active between the current year and the previous year
  actors_status_active AS (
    SELECT
      actor,
      actor_id,
      films,
      current_year,
      is_active,
      quality_class,
      is_active_last_year,
      prev_year,
      CASE
        WHEN prev_year IS NULL THEN 0 
        WHEN current_year - prev_year = 1 AND is_active = is_active_last_year THEN 0 
        ELSE 1 
      END AS did_change 
    FROM
      actors_lag
  ),
  --calculates a streak (streaked) of consecutive active years
  actors_streaked_active AS (
    SELECT
      actor,
      actor_id,
      current_year,
      is_active,
      films,
      quality_class,
      CASE
        WHEN current_year - COALESCE(prev_year, current_year - 1) = 1 THEN COALESCE(prev_year, current_year - 1) + 1
        ELSE 0
      END AS streaked
    FROM
      actors_status_active
  ),
  final AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      ROW_NUMBER() OVER (
        PARTITION BY actor_id
        ORDER BY
          CASE quality_class
            WHEN 'star' THEN 1
            WHEN 'good' THEN 2
            WHEN 'average' THEN 3
            WHEN 'bad' THEN 4
          END
      ) AS rn, 
      is_active,
      MIN(current_year) OVER (PARTITION BY actor_id, streaked) AS start_date,
      MAX(current_year) OVER (PARTITION BY actor_id, streaked) AS end_date
    FROM
      actors_streaked_active
  )
SELECT
  actor,
  actor_id,
  quality_class,
  is_active,
  start_date,
  end_date,
  end_date + 1 AS current_year
FROM
  final
WHERE rn = 1
ORDER BY actor_id, current_year