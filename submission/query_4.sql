--Actors History SCD Table Batch Backfill Query (query_4)

-- Inserting data into the "actors_history_scd" table

insert into  sanniepatron.actors_history_scd

-- CTE for lagged data, determining if actors were active in the last year
with lagged as (
select 
actor,
actor_id,
quality_class,
case when is_active then true else false end as is_active,
case when LAG(is_active,1)  over (partition by actor order by current_year) then true  else false  end as is_active_last_year,
LAG(quality_class,1)  over (partition by actor order by current_year) as quality_class_last_year,
current_year

from sanniepatron.actors
),

-- CTE for streaked data, identifying streaks of activity changes
streaked as(
select
*,
  sum(case when is_active <> is_active_last_year or quality_class <> quality_class_last_year then 1 else 0 end) over (partition by actor order by current_year) as streak_identifier
   
from lagged
)

select
actor,
actor_id,
max(quality_class) as quality_class,
max(is_active)      as is_active,
min(current_year)   as start_year,
max(current_year)   as end_year,
1917                as current_year
from streaked 
group by actor, actor_id, streak_identifier,quality_class