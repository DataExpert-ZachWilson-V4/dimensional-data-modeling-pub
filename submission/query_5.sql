--query_5

INSERT INTO hdamerla.actors_history_scd
WITH last_year_scd AS (
  SELECT * FROM hdamerla.actors_history_scd
  WHERE current_year = 2021
),
current_year_scd AS (
  SELECT * FROM hdamerla.actors
  WHERE current_year = 2022
),
combined AS (
SELECT 
  COALESCE(ls.actor, cs.actor) as actor,
  COALESCE(ls.actor_id, cs.actor_id) as actor_id,
  COALESCE(ls.start_date, cs.current_year) as start_date,
  COALESCE(ls.end_date, cs.current_year) as end_date,
  CASE 
    WHEN ls.is_active <> cs.is_active THEN 1
    WHEN ls.is_active <> cs.is_active THEN 0
  END AS did_change,
  ls.is_active as is_active_last_year,
  cs.is_active as is_active_this_year,
  2022 AS current_year,
  ls.quality_class
FROM last_year_scd ls
FULL OUTER JOIN current_year_scd cs
ON ls.actor = cs.actor AND
ls.end_date + 1 = cs.current_year
),
changes AS (
SELECT
  actor,
  actor_id,
  current_year,
  quality_class,
  CASE 
    WHEN did_change = 0 THEN       
         ARRAY[CAST(ROW(is_active_last_year, start_date, end_date+1) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
    WHEN did_change = 1 THEN
    ARRAY[CAST(ROW(is_active_last_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
      CAST(ROW(is_active_this_year, current_year , current_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
      ]
      WHEN did_change is NULL THEN
        ARRAY[CAST(ROW(
        COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
        end as change_array
        from combined
)

SELECT
  actor,
  actor_id,
  quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
 current_year
FROM changes
CROSS JOIN UNNEST(change_array) as arr
