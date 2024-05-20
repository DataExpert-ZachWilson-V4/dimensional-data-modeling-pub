-- incremental query to populate single year's worth of 'actors_history_scd' table, by combining previous year's SCD with new incoming data from the actor's table for this year
INSERT INTO steve_hut.actors_history_scd
-- grab only those records with an end_date of last year. Other records are already set, and will not change.
WITH last_year_scd AS (
  SELECT
    *
  FROM steve_hut.actors_history_scd
  WHERE end_date = 1917
),
-- grab current year records from actors table.
this_year_scd AS (
  SELECT
    *
  FROM steve_hut.actors
  WHERE current_year = 1918
),
-- Combine last_year and this_year CTEs.
-- Coalesce fields for those instances with new actors in this_year CTE
-- return a did_change field that checks whether quality_class or is_active has changed.
combined AS (
  SELECT 
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    COALESCE(ly.quality_class, ty.quality_class) as quality_class,
    COALESCE(ly.start_date, ty.current_year) as start_date,
    COALESCE(ly.end_date, ty.current_year) as end_date,
    CASE
      WHEN (ly.quality_class <> ty.quality_class) OR (ly.is_active <> ty.is_active) THEN 1
      WHEN (ly.quality_class = ty.quality_class) AND (ly.is_active = ty.is_active) THEN 0
    END as did_change,
    ly.quality_class as qc_last_year,
    ty.quality_class as qc_this_year,
    ly.is_active as is_active_last_year,
    ty.is_active as is_active_this_year,
    1918 as current_year
  FROM last_year_scd ly
  FULL OUTER JOIN this_year_scd ty ON ly.actor_id = ty.actor_id
    AND ly.end_date + 1 = ty.current_year
),
-- based on did_change field, return the appropriate array with the fields that need to be Updated or Inserted into actors_history_scd
changes AS (
  SELECT 
    actor,
    actor_id,
    quality_class,
    qc_last_year,
    qc_this_year,
    is_active_last_year,
    is_active_this_year,
    start_date,
    end_date,
    current_year,
    did_change,
    CASE 
      WHEN did_change = 0 THEN ARRAY[
        CAST(
          ROW(
            quality_class,
            is_active_last_year, 
            start_date, 
            end_date + 1
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
            )
        )
    ]
      WHEN did_change = 1 THEN ARRAY[
        /*
        CAST(
          ROW(
            quality_class,
            is_active_last_year, 
            start_date, 
            end_date
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
            )
        ),
        */
        CAST(
          ROW(
            quality_class,
            is_active_this_year, 
            current_year, 
            current_year
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
            )
        )
    ]
      WHEN did_change IS NULL THEN ARRAY[
        CAST(
          ROW(
            quality_class,
            COALESCE(is_active_last_year, is_active_this_year), 
            start_date, 
            end_date
          ) AS ROW(
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
            )
          )
        ]
    END AS change_array
  FROM combined
),
-- use did_change to help specifiy DML statement needed.
-- CROSS JOIN and UNNEST the arrays from changes CTE, so they can be used for Update or Insert statements to the actors_history_scd
modifications AS (
  SELECT
    c.actor,
    c.actor_id,
    --c.quality_class,
    c.qc_last_year,
    c.qc_this_year,
    c.is_active_last_year,
    c.is_active_this_year,
    c.start_date,
    c.end_date,
    --c.current_year,
    c.did_change,
    CASE
      WHEN c.did_change = 0 THEN 'UPDATE'
      WHEN c.did_change = 1 THEN 'INSERT'
      WHEN c.did_change IS NULL THEN 'INSERT'
    END as modification_type,
    arr.quality_class as arr_quality_class,
    arr.is_active as arr_is_active,
    arr.start_date as arr_start_date,
    arr.end_date as arr_end_date
  FROM changes c
  CROSS JOIN UNNEST(change_array) as arr
  ORDER BY actor_id, end_date
)
-- Insert rows into actors_history_scd, only those records that are new, or quality_class or is_active have changed.
-- Update records would have to be executed separately, to update the end_date on those records in actors_history_scd that don't have changes.
SELECT
  actor,
  actor_id,
  arr_quality_class,
  arr_is_active,
  arr_start_date,
  arr_end_date
FROM modifications
WHERE modification_type = 'INSERT'