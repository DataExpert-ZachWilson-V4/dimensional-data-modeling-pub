-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO actors_history_scd 
WITH lag AS (
    -- Calculate lag values for is_active and quality_class to track changes over time.
    SELECT
        actor,
        actor_id,
        is_active,
        LAG(is_active,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS is_active_last_season,
        quality_class,
        LAG(quality_class,1) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS quality_class_last_season,
        current_year
    FROM actors
    WHERE current_year <= 2021 -- The WHERE clause ensures that only data up to the year 2021 is considered.
),
lead AS (
    -- Calculate lead changes based on differences between consecutive rows.
    -- This helps to mark the changes when grouped by actor , actor_id 
    SELECT
        *,
        -- lead_change is the numeric indicator of when there is a change in either is_active or quality_class attributes of the actor
        -- This helps to precisely identify the points in time when changes occur.
        SUM(
            CASE
                WHEN (is_active <> is_active_last_season) OR (quality_class <> quality_class_last_season) THEN 1
                ELSE 0
            END) OVER (PARTITION BY actor, actor_id ORDER BY current_year) AS lead_change
    FROM lag
)
SELECT
    -- Select fields for actors_history_scd table.
    actor,
    actor_id,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 AS current_year
FROM lead
-- When grouping the data to calculate the start and end dates having the lead_change column makes it easier to 
-- identify distinct periods of stability and change
GROUP BY actor, actor_id, quality_class, is_active, lead_change