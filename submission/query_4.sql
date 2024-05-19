insert into sanchit.actors_history_scd
with cte as (select actor_id,
                    is_active,
                    quality_class,
                    lag(is_active, 1) over (
                        partition by
                            actor_id
                        order by
                            current_year
                        ) as is_active_ly, lag(quality_class, 1) over (
                        partition by
                            actor_id
                        order by
                            current_year
                        ) as quality_class_ly, current_year
             from sanchit.actors),
     streaked as (select *,
                         sum(
                                 case
                                     when (is_active <> is_active_ly) or (quality_class_ly <> quality_class) then 1
                                     else 0 end)
                             over (partition by actor_id order by current_year) as streak_id
                  from cte
                  order by actor_id, current_year)

select actor_id,
       quality_class,
       max(is_active)    as is_active,
--streak_id,
       min(current_year) as start_year,
       max(current_year) as end_year,
       1919              as current_year
from streaked
group by actor_id, streak_id, quality_class
order by actor_id, start_year, end_year