INSERT INTO datademonslayer.actors_history_scd
-- Step 1: Compute lagged values for 'quality_class' and 'is_active' status
WITH lagged AS (
    SELECT *,
           LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
           LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
    FROM datademonslayer.actors
),

-- Step 2: Calculate a 'streak_identifier' for each actor
streaked AS (
    SELECT *,
           SUM(
               CASE
                   WHEN quality_class <> previous_quality_class THEN 1
                   WHEN is_active <> previous_is_active THEN 1
                   ELSE 0
               END
           ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM lagged
),

-- Step 3: Find the maximum current year in the dataset
ts AS (
    SELECT MAX(current_year) as max_current_year
    FROM datademonslayer.actors
)

-- Step 4: Select and format data for insertion into the 'actors_history_scd' table
SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    DATE(CONCAT(CAST(MIN(current_year) AS VARCHAR), '-01-01')) AS start_date,
    DATE(CONCAT(CAST(MAX(current_year) AS VARCHAR), '-12-31')) AS end_date,
    MAX(ts.max_current_year) AS current_year
FROM streaked
    CROSS JOIN ts
GROUP BY actor, actor_id, quality_class, is_active, streak_identifier