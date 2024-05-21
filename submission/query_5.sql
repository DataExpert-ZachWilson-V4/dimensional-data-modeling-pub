-- Insert updated and new records into the actors_history_scd table
INSERT INTO raniasalzahrani.actors_history_scd (
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
)
WITH previous_year AS (
    -- Retrieve data from the previous year's SCD table
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM
        raniasalzahrani.actors_history_scd
    WHERE
        end_date = DATE '9999-12-31'
),
current_year AS (
    -- Aggregate actor films for the current year
    SELECT
        actor,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        TRUE AS is_active,
        DATE_TRUNC('day', CAST('2024-01-01' AS DATE)) AS start_date,
        DATE '9999-12-31' AS end_date
    FROM
        bootcamp.actor_films
    WHERE
        year = 2024
    GROUP BY
        actor
),
closed_out_records AS (
    -- Close out previous year records for actors present in the current year
    SELECT
        p.actor,
        p.quality_class,
        p.is_active,
        p.start_date,
        DATE_TRUNC('day', CAST('2024-01-01' AS DATE)) - INTERVAL '1' DAY AS end_date
    FROM
        previous_year p
    WHERE
        p.actor IN (SELECT actor FROM current_year)
),
combined_data AS (
    -- Combine closed out records and current year records
    SELECT * FROM closed_out_records
    UNION ALL
    SELECT * FROM current_year
)
-- Insert the combined data into the actors_history_scd table
SELECT
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    combined_data
