-- incremental query that can populate a single year, in this case 2015, to the SCD table
INSERT INTO devpatel18.actors_history_scd
WITH
    -- last year's SCD data, year = 2014
  last_year_scd AS (
    SELECT
      *
    FROM
      devpatel18.actors_history_scd
    WHERE
      current_year = 2014
  ),

-- Current year SCD
  current_year_scd AS (
    SELECT
      *,
      CASE 
        WHEN is_active THEN 1 
        ELSE 0 
      END AS is_active_flag
    FROM
      devpatel18.actors
    WHERE
      current_year = 2015
  ),
  
 -- combining the two partitions, tracking change while combining 
  combined AS (
    SELECT
      COALESCE(ls.actor, cs.actor) AS actor,
      COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
      COALESCE(ls.start_date, cs.current_year) AS start_date,
      COALESCE(ls.end_date, cs.current_year) AS end_date,
      CASE
        WHEN (ls.is_active_flag <> cs.is_active_flag) OR 
             (ls.quality_class <> cs.quality_class) THEN 1
        WHEN (ls.is_active_flag = cs.is_active_flag) AND 
             (ls.quality_class = cs.quality_class) THEN 0
      END AS did_change, 
      ls.is_active_flag AS is_active_last_year, 
      cs.is_active_flag AS is_active_this_year,
      ls.quality_class AS quality_class_last_year, 
      cs.quality_class AS quality_class_this_year,
      2015 AS current_year
    FROM
      last_year_scd ls
      -- full outer join to include all records, new, discontinued and existing
      FULL OUTER JOIN current_year_scd cs 
        ON cs.actor = ls.actor
  ),
  
  -- tracking changes, if an actor has changed attributes, cumulatively store last year's and current year's changing data
  -- if change doesn't occur, use last year's data 
  -- when change is null, for new actors in current/ for actors discontinued, use the new data or old data
  changes AS (
    SELECT 
      actor,
      actor_id,
      current_year,
      CASE 
        WHEN did_change = 0 THEN 
          ARRAY[CAST(
            ROW(
              quality_class_last_year,
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )]
        WHEN did_change = 1 THEN 
          ARRAY[
            CAST(
              ROW(
                quality_class_last_year,
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
            CAST(
              ROW(
                quality_class_this_year,
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
        WHEN did_change IS NULL THEN 
          ARRAY[CAST(
            ROW(
              COALESCE(quality_class_last_year, quality_class_this_year),
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )]
      END AS change_array
    FROM 
      combined
  )

SELECT 
  actor,
  actor_id,
  arr.quality_class,
  CASE
   WHEN arr.is_active THEN 1
   ELSE 0
  END AS is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM 
  changes 
CROSS JOIN 
  UNNEST(change_array) AS arr
