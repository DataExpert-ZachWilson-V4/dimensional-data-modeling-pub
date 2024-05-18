-- Insert data into the actors_history_scd table
INSERT INTO videet.actors_history_scd
WITH 
-- Define a CTE to fetch previous year's active status for each actor
last_active_year AS (
    SELECT
        actor_id,            -- Unique identifier for the actor
        quality_class,       -- Quality class of the actor
        current_year,        -- The year the record applies to
        -- Check the actor's active status in the previous year using LAG
        CASE
            WHEN LAG(is_active, 1) OVER (
                PARTITION BY actor_id ORDER BY current_year
            ) THEN 1
            ELSE 0
        END as is_active_last_year,
        -- Current year active status as boolean integer
        CASE
            WHEN is_active THEN 1
            ELSE 0
        END as current_is_active
    FROM
        videet.actors
),
-- Define a CTE to calculate if there has been a change in active status
streaked AS (
    SELECT
        actor_id,                -- Actor identifier
        quality_class,           -- Quality class of the actor
        current_year,            -- Current year of the data
        is_active_last_year,     -- Last year's active status
        current_is_active,       -- This year's active status
        -- Calculate changes in active status across years using a running sum
        SUM(
            CASE
                WHEN current_is_active <> is_active_last_year THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY actor_id
            ORDER BY current_year
        ) AS streak_identifier  -- Identifier for each change streak
    FROM
        last_active_year
)

-- Final SELECT to organize and convert the data for insertion
SELECT
    actor_id,                                        -- Unique actor identifier
    quality_class,                                   -- Actor's quality class
    CASE 
        WHEN MAX(current_is_active) = 1 THEN true 
        ELSE false 
    END AS is_active,                                -- Boolean indicating if actor is currently active
    -- Start date is the first day of the earliest year in the current streak
    DATE_PARSE(CAST(MIN(current_year) AS VARCHAR) || '-01-01', '%Y-%m-%d') AS start_date,
    -- End date is the last day of the latest year in the current streak
    DATE_PARSE(CAST(MAX(current_year) AS VARCHAR) || '-12-31', '%Y-%m-%d') AS end_date,
    current_year
FROM
    streaked
GROUP BY
    actor_id,
    quality_class,
    streak_identifier,
    current_year  -- Group by streak to separate different periods of activity
ORDER BY
    actor_id, start_date  -- Order results for clarity