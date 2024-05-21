-- SCD [INCREMENTAL LOAD] => Below is the incremental query that populates actors_history_scd table one year at a time from actors table
-- SCD fields => is_active and quality_class
-- TEMPORAL field => current_year

INSERT INTO tharwaninitin.actors_history_scd
-- Last year records
WITH last_year_scd AS (
    SELECT *
    FROM tharwaninitin.actors_history_scd
    WHERE current_year = 1913
),
-- This year records
this_year_scd AS (
    SELECT * FROM tharwaninitin.actors
    WHERE current_year = 1914
),
-- Join records from last year and this year
joined as (
    SELECT
      COALESCE(ly.actor, ty.actor) actor,
      COALESCE(ly.actor_id, ty.actor_id) actor_id,
      COALESCE(ly.start_date, ty.current_year) start_date,
      COALESCE(ly.end_date, ty.current_year) end_date,
      CASE
          WHEN ly.is_active != ty.is_active OR ly.quality_class != ty.quality_class THEN 1
          WHEN ly.is_active = ty.is_active AND ly.quality_class = ty.quality_class THEN 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      ty.is_active AS is_active_current_year,
      ly.quality_class AS quality_class_last_year,
      ty.quality_class AS quality_class_current_year,
      1914 as current_year
    FROM last_year_scd as ly
    FULL OUTER JOIN this_year_scd as ty ON ly.actor = ty.actor AND ly.actor_id = ty.actor_id AND ly.end_date + 1 = ty.current_year
),
-- Creating a row of array type based on did_change
changes AS (
    SELECT
        actor,
        actor_id,
        current_year,
        CASE
            WHEN did_change = 0 THEN ARRAY[
                CAST(ROW(quality_class_last_year, is_active_last_year, start_date, end_date + 1) AS ROW(quality_class VARCHAR, is_active BOOLEAN,start_date INTEGER, end_date INTEGER))
            ]
            WHEN did_change = 1 THEN ARRAY[
                CAST(ROW(quality_class_last_year, is_active_last_year, start_date, end_date + 1) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
                CAST(ROW(quality_class_current_year, is_active_current_year, current_year, current_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
            ]
            WHEN did_change IS NULL THEN ARRAY[
                CAST(ROW(COALESCE(quality_class_last_year, quality_class_current_year), COALESCE(is_active_last_year, is_active_current_year), start_date, end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
            ]
        END AS change_array
    FROM joined
)
-- Unnest the change_array and aggregate to insert into the final table
SELECT
    actor,
    actor_id,
    quality_class,
    is_active,
    MIN(start_date) as start_date,
    MAX(end_date) as end_date,
    current_year
FROM changes CROSS JOIN UNNEST (change_array)
GROUP BY actor, actor_id, quality_class, is_active, current_year