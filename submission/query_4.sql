WITH lagged AS (
  SELECT
    a.actor,
    a.is_active,
    LAG(a.is_active, 1) OVER (PARTITION BY a.actor ORDER BY current_year) AS is_active_last_year,
    LAG(a.quality_class, 1) OVER (PARTITION BY a.actor ORDER BY current_year) AS quality_class_last_year,
    a.current_year
  FROM sravan.actors AS a
  WHERE a.current_year <= 2020 -- Adjust this condition as needed
),
streaked AS (
  SELECT
    *,
    SUM(
      CASE
        WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year
        THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
  FROM lagged
)
-- Try removing the semicolon after WITH if necessary
INSERT INTO sravan.actors_history_scd (
  actor_id,
  is_active,
  start_date,
  end_date,
  quality_class
)
SELECT
  actor,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  MAX(quality_class) AS quality_class
FROM streaked
GROUP BY actor, streak_identifier;
