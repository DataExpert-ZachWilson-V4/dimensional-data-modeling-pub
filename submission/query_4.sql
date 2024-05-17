-- Insert data into the actors_history_scd table
INSERT INTO rajkgupta091041107.actors_history_scd
-- Common Table Expressions (CTEs) to prepare data for insertion
WITH 
-- CTE to get the previous active status of actors
prev_active AS (
    SELECT 
        actor_id,
        quality_class,
        -- Determine the quality class of the actor in the previous year
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_quality_class_last_year,
        is_active,
        -- Determine the active status of the actor in the previous year
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
        current_year
    FROM rajkgupta091041107.actors
    WHERE current_year < 2021
),
-- CTE to assign a change identifier based on changes in quality class or active status
identifier AS (
    SELECT *,
        SUM(
            CASE 
                WHEN is_active <> is_active_last_year OR quality_class <> is_quality_class_last_year THEN 1 
                ELSE 0 
            END
        ) OVER (PARTITION BY actor_id ORDER BY current_year) AS change_identifier
    FROM prev_active
),
-- CTE to aggregate results and determine start/end dates
results AS (
    SELECT 
        actor_id,
        quality_class,
        is_active,
        -- Determine the start date of each actor's status
        MIN(current_year) AS start_date,
        -- Determine the end date of each actor's status
        MAX(current_year) AS end_date,
        -- Record the modified date for each entry
        current_date AS modified_year
    FROM identifier
    GROUP BY actor_id, quality_class, is_active, change_identifier
)
-- Select and format the final results for insertion
SELECT 
    actor_id,
    quality_class,
    is_active,
    -- Convert start date to the first day of the year
    CAST(CAST(start_date AS VARCHAR) || '-01-01' AS DATE) AS start_date,
    -- Convert end date to the last day of the year
    CAST(CAST(end_date AS VARCHAR) || '-12-31' AS DATE) AS end_date,
    -- Record the modified date for each entry
    current_date AS modified_date
FROM results
