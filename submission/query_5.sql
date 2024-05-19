-- Populate a single year's worth of the actors_history_scd table incrementally
INSERT INTO actors_history_scd
-- Common Table Expression (CTE) to fetch data for the previous year
WITH previous_year_scd AS (
    SELECT * FROM alissabdeltoro.actors_history_scd
    WHERE current_year = 2020  -- Fetch data for the previous year (2020)
),
-- Common Table Expression (CTE) to fetch data for the current year
current_year_scd AS (
    SELECT * FROM alissabdeltoro.actors_history_scd
    WHERE current_year = 2021  -- Fetch data for the current year (2021)
),
-- Common Table Expression (CTE) to combine data from the previous year with the current year
combined_data AS (
    SELECT 
        COALESCE(py.actor_id, cy.actor_id) AS actor_id,
        COALESCE(py.actor_name, cy.actor_name) AS actor_name,
        COALESCE(py.start_date, cy.current_year) AS start_date,
        COALESCE(py.end_date, cy.current_year) AS end_date,
        CASE 
            WHEN py.is_active <> cy.is_active THEN 1  -- If is_active values differ between years, set did_change to 1
            WHEN py.is_active = cy.is_active THEN 0  -- If is_active values are the same between years, set did_change to 0
        END AS did_change,
        py.is_active AS is_active_last_year,
        cy.is_active AS is_active_this_year,
        2021 AS current_year
    FROM previous_year_scd py
    FULL OUTER JOIN current_year_scd cy
        ON py.actor_id = cy.actor_id 
        AND py.end_date + 1 = cy.current_year  -- Adjusted condition to match end_date correctly
),
-- Common Table Expression (CTE) to construct arrays representing changes in is_active status
change_array AS (
    SELECT
        actor_id,
        current_year,
        CASE 
            WHEN did_change = 0 THEN ARRAY[ROW(is_active_last_year, start_date, end_date + 1)]  -- Extend the end_date by 1 for unchanged records
            WHEN did_change = 1 THEN ARRAY[ROW(is_active_last_year, start_date, end_date),
                ROW(is_active_this_year, current_year, current_year)]  -- Add a new record for changed is_active status
            WHEN did_change IS NULL THEN ARRAY[ROW(COALESCE(is_active_last_year, is_active_this_year), start_date, end_date)]  -- Handle NULL cases
        END AS change_array
    FROM combined_data
)
-- Main query to unnest the change_array and select individual records
SELECT 
    actor_id, 
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM change_array
CROSS JOIN UNNEST(change_array.change_array) AS arr
