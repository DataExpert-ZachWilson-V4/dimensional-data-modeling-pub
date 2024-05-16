--backfill query that can populate the entire actors_history_scd table in a single query
Insert into vaishnaviaienampudi83291.actors_history_scd
with lagged as (
select actor_id, 
quality_class,
case when is_active then 1 else 0 end as is_active, -- assign 1 if the actor is active else 0
case when lag(is_active,1) over (partition by actor_id order by current_year) then 1 else 0 end as is_active_last_year, --calculate whether actor is active previous year or not
lag(quality_class,1) over (partition by actor_id order by current_year) as quality_class_last_year,
current_year 
from vaishnaviaienampudi83291.actors
)  --Get the entire history from actors table with above columns.
,
streaked as (
select *, 
sum(
    case when (is_active <> is_active_last_year or quality_class <> quality_class_last_year) then 1 else 0 end) over (partition by actor_id order by current_year) as streak_identifier 
from lagged -- here we are checking if the status of actor  & quality class is same as previous year or not. This helps to group the inactive seasons together. 
)
select actor_id, quality_class,
max(is_active)=1,
min(current_year) as start_date, max(current_year) as end_date,
2007 as current_year -- we can use the max(year) from actors table.
from streaked 
group by actor_id, streak_identifier, quality_class  -- here we are grouping by actor_id, streak_identifier and quality class to extract start and end_dates for every change.