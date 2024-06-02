

WITH current_year_data AS (
    SELECT
       *
    FROM actors
    WHERE current_year = 1984
),
previous_scd_data AS (
    SELECT 
       *
    FROM actors_history_scd
    where current_year = 1983
),

SELECT 
    actor
    quality_class,
    is_active,
    current_year AS start_date,
    NULL AS end_date
FROM current_year_data
WHERE NOT EXISTS (
    SELECT 1 
    FROM previous_scd_data
    WHERE current_year_data.actor_id = previous_scd_data.actor_id
      AND current_year_data.quality_class = previous_scd_data.quality_class
      AND current_year_data.is_active = previous_scd_data.is_active
)
UNION ALL
SELECT 
    actor
    quality_class,
    is_active,
    start_date,
    end_date
FROM previous_scd_data
WHERE actor NOT IN (
    SELECT actor
    FROM current_year_data
)
UNION ALL
SELECT 
    current_year_data.actor,
    current_year_data.quality_class,
    current_year_data.is_active,
    previous_scd_data.start_date,
    current_year - 1 AS end_date
FROM current_year_data
JOIN previous_scd_data
ON current_year_data.actor = previous_scd_data.actor
AND current_year_data.quality_class != previous_scd_data.quality_class
OR current_year_data.is_active != previous_scd_data.is_active
