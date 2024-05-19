--Actors History SCD Table Incremental Backfill Query (query_5)

insert into sanniepatron.actors_history_scd

-- Common Table Expressions (CTEs) for processing last year's and this year's data
-- CTE for last year's data in the actors_history_scd table
WITH last_year_scd AS (  
    select * from sanniepatron.actors_history_scd
    where current_year = 1917
  
    
),

-- CTE for this year's data in the actors table
this_year_scd AS (  --THIS YEAR
   select * from sanniepatron.actors
    where current_year = 1918
),

-- CTE for combining last year's and this year's data
combined as(

select
coalesce(ly.actor,ty.actor)                 as actor,
coalesce(ly.actor_id,ty.actor_id)           as actor_id,
coalesce(ly.start_date, ty.current_year)    as start_date,
coalesce(ly.end_date, ty.current_year)      as end_date,
CASE
when ly.is_active <> ty.is_active then 1
when ly.is_active = ty.is_active then 0
end                                         as did_change,

ly.is_active                                as is_active_last_season,
ty.is_active                                as is_active_this_season,
1918                                        as current_year
from last_year_scd ly
full outer join this_year_scd ty
on ly.actor= ty.actor
and ly.end_date + 1 = ty.current_year
),

-- CTE for identifying changes in activity status
changes as (
select 
actor,
actor_id,
case 
    when did_change = 0 then ARRAY[ cast(ROW(is_active_last_season, start_date,end_date + 1 ) as row(is_active boolean, start_date integer, end_date integer))]
    
    when did_change = 1 then ARRAY[
                                    cast(ROW(is_active_last_season , start_date, end_date) as row(is_active boolean, start_date integer, end_date integer)),
                                    cast(ROW(is_active_last_season , current_year, current_year) as row(is_active boolean, start_date integer, end_date integer))
                            ]
  when did_change is null then ARRAY[cast(ROW(coalesce(is_active_last_season, is_active_this_season),start_date ,end_date) as row(is_active boolean, start_date integer, end_date integer))]
                                        
                                        
                                        end as change_array,
current_year

from combined
)

select
actor,
actor_id,
arr.is_active,
arr.start_date,
arr.end_date,
current_year

from changes
cross join unnest (change_array) as arr
