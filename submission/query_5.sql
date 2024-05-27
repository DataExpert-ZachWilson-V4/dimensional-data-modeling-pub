INSERT INTO nonasj.actors_history_scd
WITH lag_scd AS (
  SELECT actor_id, actor,
    current_year,
    is_active,
    LAG(is_active) OVER(PARTITION BY actor_id, actor ORDER BY current_year) is_active_last_year,
    quality_class,
    LAG(quality_class) OVER(PARTITION BY actor_id, actor ORDER BY current_year) quality_class_last_year
  FROM nonasj.actors
),
streak_identified AS (
    SELECT *,
    SUM(CASE
            WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year
            THEN 1
            ELSE 0
        END
    ) OVER (PARTITION BY actor_id, actor ORDER BY current_year) streak_identifier
FROM lag_scd
)
SELECT actor_id, actor, quality_class, is_active,
    MIN(current_year) start_year,
    MAX(current_year) end_year,
    1919 current_year
FROM streak_identified
GROUP BY actor_id, actor, is_active, quality_class, streak_identifier