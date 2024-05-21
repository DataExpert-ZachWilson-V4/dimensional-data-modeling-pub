-- Insert the historical data into the actors_history_scd table
INSERT INTO raniasalzahrani.actors_history_scd (
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
)
WITH
-- Select the current year's data from the actors table
current_year_data AS (
    SELECT
        actor,
        quality_class,
        is_active,
        -- Set the start_date to the first day of the specified year
        DATE_TRUNC('day', CAST('2024-01-01' AS DATE)) AS start_date,
        -- Set the end_date to a far future date to indicate it is the current record
        DATE '9999-12-31' AS end_date
    FROM
        raniasalzahrani.actors
    WHERE
        current_year = 2024
),
-- Select the records from actors_history_scd that need to be closed out
closed_out_scd AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        -- Set the end_date to the day before the new year's start_date
        DATE_TRUNC('day', CAST('2024-01-01' AS DATE)) - INTERVAL '1' DAY AS end_date
    FROM
        raniasalzahrani.actors_history_scd
    WHERE
        end_date = DATE '9999-12-31'
        AND actor IN (
            -- Ensure we only close out records that have a corresponding actor in the current year data
            SELECT
                actor
            FROM
                current_year_data
        )
)
-- Combine the closed out records and the current year data and insert them into the actors_history_scd table
SELECT
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    closed_out_scd
UNION ALL
SELECT
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    current_year_data
