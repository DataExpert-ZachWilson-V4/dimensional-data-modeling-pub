INSERT INTO datademonslayer.actors_history_scd
-- Step 1: Select data for the specified year (1915 in this example)
WITH latest_data AS (
    SELECT *
    FROM datademonslayer.actors
    WHERE current_year = 1915-- This should be parameterized for dynamic year handling
),

-- Step 2: Select current active records from the history table
previous_data AS (
    SELECT *
    FROM datademonslayer.actors_history_scd
    WHERE end_date = DATE '9999-12-31'  -- Current active records
),

-- Step 3: Detect changes between current and previous data
detected_changes AS (
    SELECT
        ld.actor_id,
        ld.actor,
        ld.quality_class,
        ld.is_active,
        CASE
            WHEN pd.actor_id IS NULL OR ld.quality_class <> pd.quality_class OR ld.is_active <> pd.is_active THEN DATE(CONCAT(CAST(ld.current_year AS VARCHAR), '-01-01'))
            ELSE pd.start_date
        END AS start_date,
        CASE
            WHEN pd.actor_id IS NULL OR ld.quality_class <> pd.quality_class OR ld.is_active <> pd.is_active THEN DATE '9999-12-31'
            ELSE pd.end_date
        END AS end_date,
        ld.current_year as current_year
    FROM latest_data AS ld
    LEFT JOIN previous_data AS pd ON ld.actor_id = pd.actor_id
)

-- Step 4: Select the detected changes to be inserted into the history table
SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM detected_changes
WHERE start_date = DATE(CONCAT(CAST(current_year AS VARCHAR), '-01-01')) OR end_date = DATE '9999-12-31'