-- Populate a single year's worth of the actors_history_scd table incrementally
INSERT INTO alissabdeltoro.actors_history_scd (actor_id, actor_name, quality_class, is_active, start_date, end_date, current_year)

-- Common Table Expression (CTE) to fetch data for the previous year
WITH previous_year_scd AS (
    SELECT * FROM alissabdeltoro.actors_history_scd
    WHERE current_year = 2020  -- Fetch data for the previous year (2020)
),
-- Common Table Expression (CTE) to fetch data for the current year
current_year_scd AS (
    SELECT * FROM alissabdeltoro.actors
    WHERE current_year = 2021  -- Fetch data for the current year (2021)
),
-- Common Table Expression (CTE) to combine data from the previous year with the current year
combined_data AS (
    SELECT 
        COALESCE(py.actor_id, cy.actor_id) AS actor_id,
        COALESCE(py.actor_name, cy.actor) AS actor_name,
        COALESCE(py.quality_class, cy.quality_class) AS quality_class,
        COALESCE(YEAR(py.start_date), cy.current_year) AS start_date_year,
        COALESCE(YEAR(py.end_date), cy.current_year) AS end_date_year,
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
        AND (YEAR(py.end_date) + 1) = cy.current_year  -- Adjusted condition to match end_date correctly
),
-- Common Table Expression (CTE) to construct arrays representing changes in is_active status
changes AS (
    SELECT
        actor_id,
        actor_name,
        quality_class,
        current_year,
        CASE 
            WHEN did_change = 0 
            THEN ARRAY[
                CAST(ROW(is_active_last_year, start_date_year, end_date_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
             ]
            WHEN did_change = 1 
            THEN ARRAY[
                CAST(ROW(is_active_last_year, start_date_year, end_date_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
                CAST(ROW(is_active_this_year, current_year, current_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
            ]
            WHEN did_change IS NULL
            THEN ARRAY[
                CAST(ROW(COALESCE(is_active_last_year, is_active_this_year), start_date_year, end_date_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
            ]
        END AS change_array
    FROM combined_data
)
-- Main query to unnest the change_array and select individual records
SELECT 
    actor_id, 
    actor_name,
    quality_class,
    change_array.is_active,
    CAST(CONCAT(CAST(change_array.start_date AS VARCHAR), '-01-01') AS DATE) AS start_date,  -- Start date set to January 1st of the year
    CAST(CONCAT(CAST(change_array.end_date AS VARCHAR), '-12-31') AS DATE) AS end_date,  -- End date set to December 31st of the year
    current_year
FROM changes
CROSS JOIN UNNEST(changes.change_array) AS change_array
