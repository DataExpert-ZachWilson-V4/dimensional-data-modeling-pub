INSERT INTO actors_history_scd
WITH lagged_activity AS (
  SELECT
    actor,
    is_active,
    LAG(is_active, 1) OVER(
      PARTITION BY actor
      ORDER BY current_year
    ) AS is_active_last_year,
    quality_class,
    LAG(quality_class, 1) OVER(
      PARTITION BY actor
      ORDER BY current_year
    ) AS quality_class_last_year,
    current_year
  FROM actors
  WHERE current_year <= 2016
),
streak_change AS (
  SELECT
    actor,
    current_year,
    is_active,
    quality_class,
    SUM(
      CASE
        WHEN is_active != is_active_last_year THEN 1
        WHEN quality_class != quality_class_last_year THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY actor ORDER BY current_year
    ) AS streak_identifier
  FROM lagged_activity
)
SELECT
  actor,
  MAX(is_active) AS is_active,
  MAX(quality_class) AS quality_class,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2016 AS current_year
FROM streak_change
GROUP BY
  actor,
  streak_identifier

-- Testing the output table
-- SELECT *
-- FROM actors_history_scd
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year, start_date
