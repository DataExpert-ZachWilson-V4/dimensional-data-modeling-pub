INSERT INTO jsgomez14.actors_history_scd
WITH last_year_scd AS ( -- Define a CTE named last_year_scd.
  SELECT *  -- This contains SCD data from the previous year.
  FROM jsgomez14.actors_history_scd
  WHERE end_year = 1917
),
current_year_scd AS (
  SELECT *
  FROM jsgomez14.actors -- Define a CTE named current_year_scd.
  WHERE current_year = 1918 -- This contains data from the current year to be loaded.
),
combined AS (
  SELECT
    COALESCE(LY.actor_id, CY.actor_id) AS actor_id,
    COALESCE(LY.start_year, CY.current_year) AS start_year,
    COALESCE(LY.end_year, CY.current_year) AS end_year,
    CASE
        WHEN LY.is_active != CY.is_active
          OR LY.quality_class != CY.quality_class THEN 1
        WHEN LY.is_active = CY.is_active
          AND LY.quality_class = CY.quality_class THEN 0
    END AS did_change,
    -- We check if any of the dimensions we're tracking changed.
    LY.is_active AS is_active_last_year,
    CY.is_active AS is_active_current_year,
    LY.quality_class AS quality_class_last_year,
    CY.quality_class AS quality_class_current_year
  FROM last_year_scd AS LY
  FULL OUTER JOIN current_year_scd AS CY
    ON LY.actor_id = CY.actor_id 
      AND LY.end_year + 1 = CY.current_year
),
changes AS (
  SELECT
    actor_id,
    CASE
        -- Group SCD dimension data into an array.
        -- Based if the dimension changed, didn't change, or is a new record.
        WHEN did_change = 0
          THEN ARRAY[
            CAST(
              ROW(
                is_active_last_year,
                quality_class_last_year,
                start_year,
                end_year+1
              )
              AS
              ROW(
                is_active BOOLEAN,
                quality_class VARCHAR,
                start_year INTEGER,
                end_year INTEGER
              )
            )
          ]
       WHEN did_change = 1
         THEN ARRAY[
           CAST(
             ROW(
               is_active_last_year,
               quality_class_last_year,
               start_year,
               end_year
             )
             AS
             ROW(
               is_active BOOLEAN,
               quality_class VARCHAR,
               start_year INTEGER,
               end_year INTEGER
             )
           )
         ]
       WHEN did_change IS NULL
         THEN ARRAY[
           CAST(
             ROW(
               COALESCE(is_active_last_year, is_active_current_year),
               COALESCE(quality_class_last_year, quality_class_current_year),
               start_year,
               end_year
             )
             AS
             ROW(
               is_active BOOLEAN,
               quality_class VARCHAR,
               start_year INTEGER,
               end_year INTEGER
             )
           )
         ]
    END AS change_array
  FROM combined
)
SELECT
  actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_year,
  arr.end_year
FROM changes
CROSS JOIN UNNEST(change_array) AS arr
-- Unnest the array to get the individual records.