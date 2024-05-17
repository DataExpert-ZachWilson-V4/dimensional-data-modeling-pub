insert into harathi.actors_history_scd
with last_year As
(
  Select * from harathi.actors_history_scd
  where current_year = 2021
),
this_year As
(
  Select * from harathi.actors
  where current_year = 2022
),
combined As
(
Select
  COALESCE(ly.actor,ty.actor) As actor,
  COALESCE(ly.actor_id,ty.actor_id) As actor_id,
  COALESCE(ly.start_date, ty.current_year) As start_date,
  COALESCE(ly.end_date, ty.current_year) As end_date,
  -- check if active or quality_class changed
  CASE
    WHEN (ty.is_active <> ly.is_active) OR (ty.quality_class <> ly.quality_class) THEN 1
    WHEN (ty.is_active = ly.is_active) AND (ty.quality_class = ly.quality_class) THEN 0
  END As did_change,
  ty.is_active As is_active_this_year,
  ly.is_active As is_active_last_year,
  ty.quality_class As quality_class_this_year,
  ly.quality_class As quality_class_last_year,
  2022 As current_year
from last_year ly FULL OUTER JOIN this_year ty on ly.actor = ty.actor and ly.actor_id = ty.actor_id and
ly.end_date + 1 = ty.current_year
),

changed As
(
Select
  actor,
  actor_id,
  current_year,
  CASE
    WHEN did_change = 0 THEN ARRAY[
    CAST(
    ROW(
    quality_class_last_year,
    is_active_last_year,
    start_date,
    end_date + 1
    ) As ROW(
      quality_class VARCHAR,
      is_active boolean,
      start_date Integer,
      end_date Integer
      )
      )
      ]
    WHEN did_change = 1 THEN ARRAY[
    CAST(
    ROW(
    quality_class_last_year,
    is_active_last_year,
    start_date,
    end_date
    ) As ROW(
      quality_class VARCHAR,
      is_active boolean,
      start_date Integer,
      end_date Integer
      )
      ),
    CAST(
    ROW(
    quality_class_this_year,
    is_active_this_year,
    current_year,
    current_year
    ) As ROW(
      quality_class VARCHAR,
      is_active boolean,
      start_date Integer,
      end_date Integer
      )
      )
      ]
    WHEN did_change Is Null THEN ARRAY[
    CAST(
    ROW(
    COALESCE(quality_class_last_year,quality_class_this_year),
    COALESCE(is_active_this_year,is_active_last_year),
    start_date,
    end_date
    ) As ROW(
      quality_class VARCHAR,
      is_active boolean,
      start_date Integer,
      end_date Integer
      )
      )
      ]
  END As change_array
from combined
  )
  Select
  actor_id,
  actor,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
  from changed CROSS JOIN UNNEST (change_array) arr
