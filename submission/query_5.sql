INSERT INTO ebrunt.actors_history_scd WITH last_year AS (
  SELECT 
    * 
  FROM 
    ebrunt.actors_history_scd 
  WHERE 
    processed_year = 1960
), 
this_year AS (
  SELECT 
    * 
  FROM 
    ebrunt.actors 
  WHERE 
    current_year = 1961
), 
combined AS (
  SELECT 
    COALESCE(ly.actor_id, ty.actor_id) as actor_id, 
    COALESCE(ly.start_date, ty.current_year) as start_date, 
    COALESCE(ly.end_date, ty.current_year) as end_date, 
    ly.is_active as is_active_last_year, 
    ty.is_active as is_active_this_year, 
    ly.quality_class as quality_class_last_year, 
    ty.quality_class as quality_class_this_year, 
    CASE WHEN ly.is_active <> ty.is_active 
    OR ly.quality_class <> ty.quality_class THEN 1 WHEN ly.is_active = ty.is_active 
    AND ly.quality_class = ty.quality_class THEN 0 ELSE NULL END as did_change, 
    current_year 
  FROM 
    last_year as ly FULL 
    OUTER JOIN this_year as ty ON ly.actor_id = ty.actor_id 
    AND end_date + 1 = current_year
), 
final AS (
  SELECT 
    actor_id, 
    CASE WHEN did_change = 0 THEN ARRAY[CAST(
      ROW(
        quality_class_last_year, is_active_last_year, 
        start_date, end_date + 1
      ) AS ROW(
        quality_class VARCHAR, is_active BOOLEAN, 
        start_date INTEGER, end_date INTEGER
      )
    ) ] WHEN did_change = 1 THEN ARRAY[ CAST(
      ROW(
        quality_class_last_year, is_active_last_year, 
        start_date, end_date
      ) AS ROW(
        quality_class VARCHAR, is_active BOOLEAN, 
        start_date INTEGER, end_date INTEGER
      )
    ), 
    CAST(
      ROW(
        quality_class_this_year, is_active_this_year, 
        current_year, current_year
      ) AS ROW(
        quality_class VARCHAR, is_active BOOLEAN, 
        start_date INTEGER, end_date INTEGER
      )
    ) ] WHEN did_change IS NULL THEN ARRAY[CAST(
      ROW(
        COALESCE(
          quality_class_last_year, quality_class_this_year
        ), 
        COALESCE(
          is_active_last_year, is_active_this_year
        ), 
        start_date, 
        end_date
      ) AS ROW(
        quality_class VARCHAR, is_active BOOLEAN, 
        start_date INTEGER, end_date INTEGER
      )
    ) ] END as changes 
  FROM 
    combined
) 
SELECT 
  actor_id, 
  arr.quality_class, 
  arr.is_active, 
  arr.start_date, 
  arr.end_date, 
  1961 as current_year 
FROM 
  final CROSS 
  JOIN UNNEST(changes) as arr 
ORDER BY 
  actor_id, 
  end_date
