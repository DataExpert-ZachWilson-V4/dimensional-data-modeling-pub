-- Insert historical data into saismail.actors_history_scd
INSERT INTO saismail.actors_history_scd (
  actor_id,
  quality_class,
  is_active,
  start_date,
  end_date,
  "current_date"
)
WITH actor_changes AS (
    -- Get distinct years for which we have actor data
    SELECT DISTINCT current_year
    FROM saismail.actors
),
actor_history AS (
    -- Generate historical records for each actor for each year
    SELECT
        a.actor_id,
        a.quality_class,
        a.is_active,
        a.current_year AS start_date,
        a.current_year AS end_date,
        (SELECT EXTRACT(YEAR FROM current_date)) AS "current_date"
    FROM saismail.actors a
    JOIN actor_changes ac ON a.current_year = ac.current_year
),
final_history AS (
    SELECT
        actor_id,
        quality_class,
        is_active,
        start_date,
        end_date,
        "current_date"
    FROM actor_history
    UNION ALL
    -- Handle changes in actors' status or quality class over the years
    SELECT
        a.actor_id,
        a.quality_class,
        a.is_active,
        MIN(a.start_date) AS start_date,
        MAX(a.end_date) AS end_date,
        a."current_date"
    FROM actor_history a
    GROUP BY
        a.actor_id,
        a.quality_class,
        a.is_active,
        a."current_date"
)
-- Select final history records for insertion
SELECT
    actor_id,
    quality_class,
    is_active,
    start_date,
    end_date,
    "current_date"
FROM final_history