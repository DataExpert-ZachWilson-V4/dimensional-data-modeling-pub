/* Actors History SCD Table Incremental Backfill Query (query_5)
Write an "incremental" query that can populate a single year's worth
of the actors_history_scd table by combining the previous year's SCD
data with the new incoming data from the actors table for this year.
*/
INSERT INTO danieldavid.actors_history_scd
-- 1) LY: load values with end_date from last year
WITH last_year_scd AS (
    SELECT *
    FROM danieldavid.actors_history_scd
    WHERE current_year = 2010
),
-- 2) TY: select this year's records from the actors table
this_year AS (
    SELECT
        actor,
        actor_id,
        quality_class,
        is_active,
        current_year
  FROM danieldavid.actors
  WHERE current_year = 2011
),
-- 3) Combine: to compare changes between this year and last year data
combined AS (
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        
        -- comparison logic for next step
        ly.quality_class AS quality_class_ly,
        ty.quality_class AS quality_class_ty,
        ly.is_active AS is_active_ly,
        ty.is_active AS is_active_ty,
        CASE WHEN ly.is_active <> ty.is_active OR ly.quality_class <> ty.quality_class THEN 1
            WHEN ly.is_active = ty.is_active AND ly.quality_class = ty.quality_class THEN 0
        END AS did_change,

        start_date,
        end_date,
        2011 AS current_year

  FROM last_year_scd ly
  FULL OUTER JOIN this_year ty
  ON ly.actor_id = ty.actor_id AND (ly.end_date + 1) = ty.current_year
),
-- 4) Changes: depending on updating / addition changes, clean data to target schema
changes AS (
    SELECT
        actor,
        actor_id,
        current_year,
        CASE WHEN did_change = 0
                THEN ARRAY[
                    CAST(ROW(quality_class_ly, is_active_ly, start_date, end_date + 1)
                    AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
            WHEN did_change = 1
                THEN ARRAY[
                    CAST(ROW(quality_class_ly, is_active_ly, start_date, end_date)
                    AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
                    CAST(ROW(quality_class_ty, is_active_ty, current_year, current_year)
                    AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
            WHEN did_change IS NULL
                THEN ARRAY[
                    CAST(ROW(
                        COALESCE(quality_class_ly,quality_class_ty),
                        COALESCE(is_active_ly,is_active_ty),
                        current_year, 
                        current_year
                    ) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
        END as change_array
    FROM combined
)
-- 5) Insert: insert cleaned data into SCD table
SELECT
    actor,
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM changes
CROSS JOIN UNNEST(change_array) as arr
-- Go go chatgpt feedback! :)