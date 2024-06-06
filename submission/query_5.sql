
-- Inserting records into the actors_history_scd table

-- Common table expression to get the latest records from actors_history_scd table
WITH latest_records AS (
    SELECT actor, actor_id, quality_class, is_active, start_date, end_date
    FROM bhakti.actors_history_scd
    WHERE start_date = (
        SELECT MAX(start_date) 
        FROM bhakti.actors_history_scd 
        WHERE actor_id = actors_history_scd.actor_id
    )
),

-- Common table expression to get new records from actors table
new_records AS (
    SELECT actor, actor_id, quality_class, is_active, current_year AS start_date, NULL AS end_date
    FROM bhakti.actors
    WHERE current_year = <current_year> -- Replace <current_year> with the actual value
),

-- Common table expression to update records in latest_records with the current year as end_date
updated_records AS (
    SELECT actor, actor_id, quality_class, is_active, start_date, <current_year> AS end_date -- Replace <current_year> with the actual value
    FROM latest_records
)

-- Selecting the updated_records and new_records
SELECT * FROM updated_records
UNION ALL
SELECT * FROM new_records