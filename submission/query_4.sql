INSERT INTO raniasalzahrani.actors_history_scd (
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
)
WITH ranked_actors AS (
    -- Create a CTE to generate the start and end dates for each actor's record
    SELECT 
        actor,
        quality_class,
        is_active,
        -- Calculate the start date as the first day of the current year
        DATE_TRUNC('day', CAST(CAST(current_year AS VARCHAR) || '-01-01' AS DATE)) AS start_date,
        -- Use the LEAD function to get the start date of the next record as the end date of the current record
        LEAD(DATE_TRUNC('day', CAST(CAST(current_year AS VARCHAR) || '-01-01' AS DATE)), 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS next_start_date
    FROM raniasalzahrani.actors
)
SELECT 
    actor,
    quality_class,
    is_active,
    start_date,
    -- Set the end date to one day before the next start date, or to 9999-12-31 if it's the current record
    COALESCE(next_start_date - INTERVAL '1' DAY, DATE '9999-12-31') AS end_date
FROM ranked_actors
