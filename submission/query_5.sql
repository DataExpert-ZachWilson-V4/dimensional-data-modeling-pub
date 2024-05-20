-- ============= Query 5 ========
-- Let's test this by inserting a record for year 2022 for Tom Holland:
-- INSERT INTO andreskammerath.actors (
--     actor,
--     actor_id,
--     films,
--     quality_class,
--     is_active,
--     current_year
-- )
-- VALUES (
--     'Tom Holland',
--     'nm4043618',
--     ARRAY[
--     ],
--     'good',
--     false,
--     2022
-- )


-- CTE to fetch the latest records for all actors up to 2021
WITH latest_records AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY end_date DESC) AS rn
    FROM andreskammerath.actors_history_scd
    WHERE end_date = 2021
),

-- CTE to fetch the 2022 data for all actors
new_data_2022 AS (
    SELECT
        actor,
        quality_class,
        is_active,
        2022 AS start_date,
        2022 AS end_date  -- Assuming 'open-ended' for the new record
    FROM andreskammerath.actors
    WHERE current_year = 2022
),

-- CTE to determine whether updates are necessary
changes AS (
    SELECT
        n.actor,
        n.quality_class,
        n.is_active,
        n.start_date,
        n.end_date,
        l.quality_class AS old_quality_class,
        l.is_active AS old_is_active,
        l.end_date AS old_end_date,
        CASE
            WHEN n.quality_class = l.quality_class AND n.is_active = l.is_active THEN FALSE
            ELSE TRUE
        END AS needs_update
    FROM new_data_2022 n
    LEFT JOIN latest_records l ON n.actor = l.actor AND l.rn = 1
)

-- Need to update end_date for records didn't change
-- need to insert new record into actors_history_scd for those that did change (Tom H alone in this case)
--     -- Insert new records where changes are detected
INSERT INTO andreskammerath.actors_history_scd (actor, quality_class, is_active, start_date, end_date)
SELECT actor, quality_class, is_active, start_date, end_date
FROM changes
WHERE needs_update = TRUE
-- -- Update the existing records
-- I don't find a good way to update only column end_date for records that didn't change any dimension