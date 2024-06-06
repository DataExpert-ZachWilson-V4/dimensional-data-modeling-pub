-- Update the end_date for actors in the actors_history_scd table
UPDATE bhakti.actors_history_scd
SET end_date = current_year
WHERE actor_id IN (
    SELECT actor_id
    FROM bhakti.actors
    WHERE current_year = ---<Replace with year> 
) AND end_date IS NULL

-- Insert new records into the actors_history_scd table
INSERT INTO bhakti.actors_history_scd
SELECT 
    actor,
    actor_id,
    quality_class,
    is_active,
    current_year AS start_date,
    NULL AS end_date
FROM bhakti.actors
WHERE current_year = --<Replace with year>
