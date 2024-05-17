INSERT INTO actors_history_scd
--Tracking changes in quality class and is_active
--Incremental load of actors_history_scd
WITH last_year AS (
SELECT *
FROM actors_history_scd
WHERE current_year = 2020
)
, this_year AS (
  SELECT *
  FROM actors
  WHERE current_year = 2021
)
, combined AS (
  SELECT COALESCE(ly.actor, ty.actor) AS actor
       , COALESCE(ly.actor_id, ty.actor_id) AS actor_id
       -- get quality class from last year
      , ly.quality_class AS last_year_quality_class
      -- get quality class from current year
      , ty.quality_class AS this_year_quality_class
      -- get is_active from last year
       , ly.is_active AS is_active_last_year
      -- get is_active from current year
       , ty.is_active AS is_active_this_year
       -- create flag to track is_active and quality_class changes between last year and this year
      , CASE WHEN ly.is_active <> ty.is_active AND ly.quality_class <> ty.quality_class THEN 1
             WHEN ly.is_active = ty.is_active AND ly.quality_class = ty.quality_class THEN 0
       END AS did_change
          -- construct start date from current year and replace null
       , COALESCE(ly.start_date, DATE_PARSE(CONCAT_WS('-', ARRAY[CAST(ty.current_year AS VARCHAR), '01', '01']), '%Y-%m-%d')) AS start_date
       -- construct end date from current year and replace null
       , COALESCE(ly.end_date, DATE_PARSE(CONCAT_WS('-', ARRAY[CAST(ty.current_year AS VARCHAR), '12', '31']), '%Y-%m-%d')) AS end_date
       , 2021 AS current_year
  FROM last_year ly
  FULL OUTER JOIN this_year ty
               ON ly.actor_id = ty.actor_id
               -- filter for all last year values which when incremented by 1 equals current year
               AND YEAR(ly.end_date) + 1 = ty.current_year
)
, changes AS (
SELECT actor
      , actor_id
      , current_year
      , CASE WHEN did_change = 0 
          -- if no change the update values from lastvyear
            THEN ARRAY[CAST(ROW(is_active_last_year, last_year_quality_class, start_date, DATE_ADD('year', 1, end_date)) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date DATE, end_date DATE))]
            WHEN did_change = 1
            -- if is_active and quality class changed then get values from both last year and this year
            THEN ARRAY[CAST(ROW(is_active_last_year, last_year_quality_class, start_date, end_date) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date DATE, end_date DATE)),
            CAST(ROW(is_active_this_year, this_year_quality_class, start_date, end_date) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date DATE, end_date DATE))]
            WHEN did_change IS NULL
            -- if new value then get only current year values
            THEN ARRAY[CAST(ROW(COALESCE(is_active_last_year, is_active_this_year), COALESCE(last_year_quality_class, this_year_quality_class), start_date, end_date) AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date DATE, end_date DATE))]
        END AS change_array
FROM combined
)

SELECT actor
     , actor_id
     , quality_class
     , arr.is_active
     , arr.start_date
     , arr.end_date
     , current_year
FROM changes
-- expand cumulative column change_array
CROSS JOIN UNNEST(change_array) AS arr
