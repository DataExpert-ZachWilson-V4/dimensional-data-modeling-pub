--Query 4
-- I am running this backfill query till year 1919
insert into hariomnayani88482.actors_history_scd 
with cte as(
SELECT
  actor,
  is_active,
  quality_class,
  LAG(is_active, 1) OVER (
    PARTITION BY
      actor
    ORDER BY
      current_year
  ) AS is_active_last_year,
   LAG(quality_class, 1) OVER (
    PARTITION BY
      actor
    ORDER BY
      current_year
  ) AS quality_class_last_year,
  current_year
FROM
 hariomnayani88482.actors
 ),
 streaked as (
select *,
   sum(case when is_active<>is_active_last_year then 1 else 0 end) over (partition by actor order by current_year) as streak_ide,
   sum(case when quality_class_last_year<>quality_class then 1 else 0 end) over (partition by actor order by current_year) as quality_streak
from cte
order by actor,current_year)

select 
actor,
quality_class,
max(is_active) as is_active,
--streak_ide,
--quality_streak,
min(current_year) as start_year,
max(current_year) as end_year,
1919 as current_year
from streaked
group by actor,streak_ide,quality_streak,quality_class
order by actor,start_year,end_year
