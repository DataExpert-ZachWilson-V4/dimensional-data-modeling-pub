-- Populating the entire actors_history_scd table using backfill approach from the actors table in Trino
INSERT INTO jlcharbneau.actors_history_scd (actor_id, quality_class, is_active, start_date, end_date)
SELECT
    a.actor_id,
    a.quality_class,
    a.is_active,
    DATE(CONCAT(CAST(a.current_year AS VARCHAR), '-01-01')) AS start_date,  -- Constructing a DATE from an integer year
    COALESCE(
    date_add('day', -1, DATE(CONCAT(CAST(LEAD(a.current_year) OVER (PARTITION BY a.actor_id ORDER BY a.current_year) AS VARCHAR), '-01-01'))),
    DATE '9999-12-31'
    ) AS end_date
FROM
    jlcharbneau.actors a
ORDER BY
    a.actor_id, a.current_year