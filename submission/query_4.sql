INSERT INTO actors_history_scd (actor_id, quality_class, is_active, start_date, end_date)
SELECT 
    actor_id,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date
FROM actors
GROUP BY actor_id, quality_class, is_active
ORDER BY actor_id, start_date;
