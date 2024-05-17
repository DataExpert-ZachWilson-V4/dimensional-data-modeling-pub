INSERT INTO actors_history_scd
--Full load of actors_history_scd table by tracking changes in --is_active and quality_class
WITH lagged AS (
SELECT actor
     , actor_id
     , quality_class
    , LAG(quality_class, 1) OVER(
        PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year
     , is_active
     , LAG(is_active, 1) OVER(PARTITION BY actor, actor_id ORDER BY current_year) is_active_last_year
     , current_year
FROM actors
WHERE current_year <= 2020
)
, changed AS (
  SELECT actor
     , actor_id
     , quality_class
     , quality_class_last_year
     , is_active
     , is_active_last_year
     -- group changes together
     , SUM(CASE WHEN is_active <> is_active_last_year AND quality_class <> quality_class_last_year THEN 1 ELSE 0 END) OVER(PARTITION BY actor, actor_id ORDER BY current_year) AS status_changed
     , current_year
  FROM lagged
)

SELECT actor
     , actor_id
     , quality_class
     , is_active
     -- construct date from current_year and make it first day of year
     , MIN(DATE_PARSE(CONCAT_WS('-', ARRAY[CAST(current_year AS VARCHAR),
       '01',
       '01']
       ), '%Y-%m-%d')) AS start_date
       -- construct end date from current_year and make it point to last day of year
     , MAX(DATE_PARSE(CONCAT_WS('-',
     ARRAY[CAST(current_year AS VARCHAR),
     '12',
     '31'
     ]
     ), '%Y-%m-%d')) AS end_date
     , 2020 AS current_year
FROM changed
GROUP BY actor
     , actor_id
     , quality_class
     , is_active
     , status_changed  