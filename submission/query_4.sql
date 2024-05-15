-- "Backfill" query that can populate the entire actors_history_scd table in a single query
INSERT INTO
    mariavyso.actors_history_scd
WITH
    -- CTE to determine changes in 'is_active' status and 'quality_class'
    lagged AS (
        SELECT
            actor,
            -- Converting 'is_active' BOOLEAN to integer for easier comparison
            CASE
                WHEN is_active THEN 1
                ELSE 0
            END AS is_active,
            -- Calculating the 'is_active' status of the previous year for each actor
            CASE
                WHEN LAG(is_active, 1) OVER (
                    PARTITION BY
                        actor
                    ORDER BY
                        current_year
                ) THEN 1
                ELSE 0
            END AS is_active_last_year,
            current_year,
            quality_class,
            -- Fetching the 'quality_class' from the previous year for each actor
            LAG(quality_class, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) as last_year_quality
        FROM
            mariavyso.actors
    ),
    -- CTE to track changes over time and identify distinct periods (streaks)
    streaked AS (
        SELECT
            *,
            -- Generating an identifier for each streak based on changes in 'is_active' status
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS identifier,
            SUM(
                CASE
                    WHEN quality_class <> last_year_quality THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS quality_class_identifier
            
        FROM
            lagged
    )
    -- Final SELECT statement to aggregate the data and prepare for insertion
SELECT
    actor,
    any_value(quality_class) AS quality_class,
    -- Checking if the actor was active at any point during the streak
    MAX(is_active) = 1 AS is_active,
    -- Calculating the start and end dates of each streak
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    -- Setting the 'current_year' for all records to 2020
    2021 AS current_year
FROM
    streaked
GROUP BY
    actor,
    identifier, -- Grouping by the streak identifier to aggregate records per distinct period
    quality_class_identifier
