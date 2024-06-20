INSERT INTO changtiange199881320.actors_history_scd

-- SCD Incremental Backfill, populate a single year's data by combining the previous
-- year's SCD data with the new incoming data from the table for this year. 

-- Insert first year's data
WITH lagged AS (
    SELECT
        actor, 
        quality_class, 
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active, 
        CASE WHEN LAG(is_active, 1) OVER 
             (PARTITION BY actor ORDER BY current_year) THEN 1 
             ELSE 0 END AS is_active_last_year, 
        current_year
    FROM 
        changtiange199881320.actors
    WHERE 
        current_year <= 1914
),
streaked AS(
    SELECT 
        *, 
        SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER 
            (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM 
        lagged
)
SELECT 
    actor, 
    quality_class, 
    MAX(is_active) = 1 AS is_active, -- MAX(is_active) = 1show true/false
    MIN(current_year) AS start_date, -- MAX(is_active) only show 1/0
    MAX(current_year) AS end_date, 
    1914 AS current_year
FROM 
    streaked
GROUP BY 
    actor,
    quality_class,
    streak_identifier

------------------------------------------------------------------------------------
-- Load it incrementally
INSERT INTO changtiange199881320.actors_history_scd

WITH last_year_scd AS(
    SELECT * 
    FROM changtiange199881320.actors_history_scd
    WHERE current_year = 1914 --change
), 
current_year_scd AS (
    SELECT * 
    FROM changtiange199881320.actors
    WHERE current_year = 1915 --change
), 
combined AS(
    SELECT 
        COALESCE(ly.actor, cy.actor) AS actor,
        COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
        COALESCE(ly.start_date, cy.current_year) AS start_date,
        COALESCE(ly.end_date, cy.current_year) AS end_date, 
        CASE
            WHEN ly.is_active <> cy.is_active THEN 1
            WHEN ly.is_active = cy.is_active THEN 0
        END AS did_change, 
        ly.is_active AS is_active_last_year, -- active flags
        cy.is_active AS is_active_this_year, -- active flags
        1915 AS current_year --change
    FROM 
        last_year_scd ly
    FULL OUTER JOIN -- include new users, LEFT JOIN will not have new users
        current_year_scd cy
    ON 
        ly.actor = cy.actor AND ly.end_date + 1 = cy.current_year
), 
changes AS (
    SELECT
        actor,
        quality_class, 
        current_year,
        CASE 
            WHEN did_change = 0 THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, start_date, end_date + 1) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
            WHEN did_change = 1 THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, start_date, end_date) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                ),
                CAST(
                    ROW(is_active_this_year, current_year, current_year) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
            WHEN did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(COALESCE(is_active_last_year, is_active_this_year), 
                        start_date, end_date) AS 
                    ROW(is_active boolean, start_date integer, end_date integer)
                )
            ]
        END AS change_array
    FROM
        combined
)
SELECT
    actor, 
    quality_class, 
    arr.is_active, 
    arr.start_date,
    arr.end_date, 
    current_year
FROM
    changes
CROSS JOIN 
    UNNEST (change_array) AS arr
