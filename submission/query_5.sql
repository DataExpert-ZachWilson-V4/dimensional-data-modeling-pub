INSERT INTO ChrisTaulbee.actors_history_scd

WITH previous_year_cte AS (
    SELECT
        *
    FROM
        ChrisTaulbee.actors_history_scd
      WHERE YEAR(end_date) = latest_year
),
current_year_cte AS (
    SELECT 
        *
    FROM
        ChrisTaulbee.actors
    WHERE
        current_year = latest_year + 1
),
combined AS (
SELECT
    COALESCE(py.actor_id, cy.actor_id) as actor_id,
    latest_year + 1 as latest_year,
    py.is_active as is_active_last_year,
    cy.is_active as is_active_this_year,
    py.quality_class as quality_class_last_year,
    cy.quality_class as quality_class_this_year,
    COALESCE(py.start_date, CAST(CONCAT(CAST(cy.current_year AS VARCHAR), '-01-01') AS DATE)) AS start_date,
    COALESCE(py.end_date, CAST(CONCAT(CAST(cy.current_year AS VARCHAR), '-01-01') AS DATE)) AS end_date,
    CASE
        WHEN py.is_active <> cy.is_active THEN 1
        WHEN py.quality_class <> cy.quality_class THEN 1
        WHEN py.quality_class = cy.quality_class THEN 0
        WHEN py.is_active = cy.is_active THEN 0
    END AS did_change
FROM
    previous_year_cte py
FULL OUTER JOIN
    current_year_cte cy ON py.actor_id = cy.actor_id AND py.latest_year = cy.current_year)
, changes as (
SELECT *
    CASE
        WHEN did_change = 0 THEN ARRAY[ CAST(ROW(is_active_last_year, start_date, end_date + INTERVAL '1 year') AS ROW(is_active BOOLEAN, start_date DATE, end_date DATE)) ]
        WHEN did_change = 1 THEN ARRAY[ CAST(ROW(is_active_last_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date DATE, end_date DATE)), CAST(ROW(is_active_this_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date DATE, end_date DATE)) ]
        WHEN did_change IS NULL THEN ARRAY[ CAST(ROW(is_active_this_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date DATE, end_date DATE)) ]
        END as change_array
    FROM combined
)

SELECT
    actor_id,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    latest_year
FROM
    changes
CROSS JOIN UNNEST(change_array) as arr

