WITH lagged AS (
    SELECT
        actor_id,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS previous_quality_class,
            LAG(is_active, 1) OVER (
      PARTITION BY actor_id
      ORDER BY current_year
    ) AS previous_is_active,
            current_year
    FROM jlcharbneau.actors
),
 streaked AS (
     SELECT
         *,
         SUM(
                 CASE
                     WHEN quality_class <> previous_quality_class
                         THEN 1
                     WHEN is_active <> previous_is_active
                         THEN 1
                     ELSE 0
                     END
             ) OVER (
  PARTITION BY actor_id
  ORDER BY current_year
) AS streak_identifier
     FROM lagged
)
INSERT INTO jlcharbneau.actors_history_scd (actor_id, quality_class, is_active, start_date, end_date)
SELECT
    actor_id,
    quality_class,
    is_active,
    DATE(CONCAT(CAST(MIN(current_year) AS VARCHAR), '-01-01')) AS start_date,
    COALESCE(
    date_add('day', -1, DATE(CONCAT(CAST(MAX(current_year) AS VARCHAR), '-01-01'))),
    DATE '9999-12-31'
    ) AS end_date
FROM streaked
GROUP BY actor_id, quality_class, is_active, streak_identifier
ORDER BY actor_id, start_date