INSERT INTO ibrahimsherif.actors_history_scd
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
  FROM ibrahimsherif.actors
  WHERE current_year <= 2019
),
streak_change AS (
  SELECT
    actor,
    current_year,
    is_active,
    is_active_last_year,
    SUM(
      CASE
        WHEN is_active != is_active_last_year THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY actor ORDER BY current_year
    ) AS is_active_streak_identifier,
    quality_class,
    quality_class_last_year,
    SUM(
      CASE
        WHEN quality_class != quality_class_last_year THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY actor ORDER BY current_year
    ) AS quality_class_streak_identifier
  FROM lagged_activity
),
is_active_scd AS (
  SELECT
    actor,
    MAX(is_active) AS  is_active,
    NULL AS quality_class,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2019 AS current_year
  FROM streak_change
  GROUP BY
    actor,
    is_active_streak_identifier
),
quality_class_scd AS (
  SELECT
    actor,
    NULL AS is_active,
    MAX(quality_class) AS quality_class,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2019 AS current_year
  FROM streak_change
  GROUP BY
    actor,
    quality_class_streak_identifier
)
SELECT *
FROM is_active_scd
UNION ALL
SELECT *
FROM quality_class_scd

-- Testing the output table
-- SELECT *
-- FROM ibrahimsherif.actors_history_scd
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year
