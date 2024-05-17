-- ============= Query 5 ========
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

-- Update the existing records and insert new ones as necessary
BEGIN;
    -- Update the end date where no changes are needed
    UPDATE andreskammerath.actors_history_scd
    SET end_date = 2022
    FROM changes
    WHERE andreskammerath.actors_history_scd.actor = changes.actor
    AND changes.needs_update = FALSE
    AND andreskammerath.actors_history_scd.end_date = 2021;

    -- Insert new records where changes are detected
    INSERT INTO andreskammerath.actors_history_scd (actor, quality_class, is_active, start_date, end_date)
    SELECT actor, quality_class, is_active, start_date, end_date
    FROM changes
    WHERE needs_update = TRUE;

COMMIT;

-- How do I test this?
-- What happens if I have the same values (just need to change the year to year +1)
-- What happens if a dimension changed? (insert the row with the right year)
-- what happens if I need to change the data for a year in the middle of a range (let's take care of this later)
-- TO-DO: Need to test query 5. Probable need to insert record 2022 for Tom H and
-- test case 1 and 2 of above.