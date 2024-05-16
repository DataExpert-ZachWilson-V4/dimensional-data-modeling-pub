--Actors History SCD Table Incremental Backfill Query 
Insert into vaishnaviaienampudi83291.actors_history_scd
with last_year_scd as 
(
select * from vaishnaviaienampudi83291.actors_history_scd
where current_year = 2007 --select max date from actors_history_scd table.I have inserted only till 2007
), 
current_year_scd as (
select * from vaishnaviaienampudi83291.actors
where current_year = 2008 --get the incremental data from actors table
),
combined as (
select coalesce(ly.actor_id, cy.actor_id) as actor_id,
coalesce(ly.start_date, cy.current_year) as start_date,
coalesce(ly.end_date, cy.current_year) as end_date,
case when ly.is_active <> cy.is_active then 1  -- this helps to undertand if there is a change 
when ly.is_active = cy.is_active then 0 end as did_change,
case when ly.quality_class <> cy.quality_class then 1 
when ly.quality_class = cy.quality_class then 0 end as did_change_quality_class,
ly.is_active as is_active_last_year,
cy.is_active as is_active_current_year,
ly.quality_class as quality_class_last_year,
cy.quality_class as quality_class_current_year,
2008 as current_year
from last_year_scd ly full outer join current_year_scd cy 
on ly.actor_id = cy.actor_id
and ly.end_date + 1 = cy.current_year)
,changes as ( 
--here we are creating the required fields/changes in array
select actor_id, current_year, did_change, is_active_last_year,  is_active_current_year,
quality_class_last_year,  quality_class_current_year,
case 
when did_change = 0 and did_change_quality_class = 0
 then Array[
    CAST(
        Row(
            is_active_last_year, 
            start_date, 
            end_date + 1, 
            quality_class_last_year
            ) AS ROW(
                is_active boolean, 
                start_date integer, 
                end_date integer, 
                quality_class varchar
            )
            )] --If there is no change in status, then check if there is a change in quality_class. If no change, then update the end_date to end_date+1 (because current_year = end_date+1)
when did_change = 0 and did_change_quality_class = 1 then Array[
    CAST(ROW(is_active_last_year, start_date, end_date, quality_class_last_year) as ROW(
        is_active boolean, 
        start_date integer, 
        end_date integer, 
        quality_class varchar)), 
    CAST(ROW(is_active_last_year, current_year, current_year, quality_class_current_year) as row(
        is_active boolean, 
        start_date integer, 
        end_date integer,
         quality_class varchar
         ))] -- if there's no change in status but change in quality_class  then create a new row with new value for quality_class with new dates. 
when did_change = 1 then Array[
    CAST(Row(
        is_active_last_year, 
        start_date, 
        end_date, 
        quality_class_last_year) AS ROW(
            is_active boolean, 
            start_date integer, 
            end_date integer, 
            quality_class varchar
            )), -- if there is change in status, then the previous row remains as is and create a new row with new quality class and new start & end_dates. If there's no change in quality class, then create a new row with start, end_dates and is_Active values.
    CAST(Row(
        is_active_current_year,
         current_year, 
         current_year, 
         quality_class_current_year) AS ROW(
            is_active boolean, 
            start_date integer, 
            end_date integer, 
            quality_class varchar))] 
when did_change is null then Array[CAST(Row(
    COALESCE(is_active_last_year, is_active_current_year), 
    start_date, 
    end_date, 
    coalesce(quality_class_last_year, quality_class_current_year)
    ) as ROW(
        is_active boolean, 
        start_date integer, 
        end_date integer, 
        quality_class varchar))] --if did_changes is null, then use coalesce for tracking the values. 
end as changes_array
from combined
)
select distinct actor_id, --using distinct to avoid duplicates if there are any
arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (changes_array) AS arr