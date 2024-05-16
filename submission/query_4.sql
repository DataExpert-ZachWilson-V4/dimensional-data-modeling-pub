INSERT INTO ovoxo.actors_history_scd
WITH
  current_previous_status AS (
    SELECT
      actor_name,
      actor_id,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active_current_year,
      quality_class AS quality_class_current_year,
      CASE
        WHEN LAG(is_active, 1) OVER (PARTITION BY actor_name ORDER BY current_year) THEN 1
        ELSE 0
      END AS is_active_previous_year,
      LAG(quality_class, 1) OVER (PARTITION BY actor_name ORDER BY current_year) AS quality_class_previous_year,
      current_year
    FROM  ovoxo.actors
    WHERE 1=1
      AND current_year <= 2019
  ),
  
    change_boundary as (
        SELECT *,
        CASE
            WHEN is_active_current_year <> is_active_previous_year OR quality_class_current_year <> quality_class_previous_year THEN 1 -- check if across years is_active or quality class has changed
            ELSE 0
        END AS is_active_quality_class_changed,
        SUM(CASE
                WHEN is_active_current_year <> is_active_previous_year OR quality_class_current_year <> quality_class_previous_year THEN 1
                ELSE 0 END
        ) OVER (PARTITION BY actor_name ORDER BY current_year) AS change_boundary_identifier -- keep track of change boundaries across years if is_active or quality class has changed
        FROM current_previous_status
    )

SELECT
  actor_name,
  actor_id,
  MAX(quality_class_current_year) AS quality_class,
  MAX(is_active_current_year) = 1 AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2019 AS current_year
FROM change_boundary
GROUP BY
  actor_name,
  actor_id,
  change_boundary_identifier