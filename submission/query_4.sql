INSERT INTO richiesingh.actors_history_scd 
-- get is active and quality class from previous year using LAG for a given actor
with lagged as(
select actor, CASE WHEN is_active THEN 1 ELSE 0 END as is_active,
      CASE WHEN  LAG(is_active) OVER(PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END as is_active_last_year,
quality_class,
LAG(quality_class) OVER(PARTITION BY actor ORDER BY current_year) as quality_class_last_year, current_year
from richiesingh.actors 
WHERE current_year <= 2020
)
--find the streak to see when the status changed
, streaked as (
select *,
    SUM(CASE WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year
    THEN 1 ELSE 0 END) OVER(PARTITION BY actor ORDER BY current_year) as streak_identifier
from lagged
)

-- find the start and end date
SELECT actor,quality_class,MAX(is_active)=1 as is_active,
      min(current_year) start_date,
      max(current_year) end_date,
      2020 as current_year
from streaked
--WHERE actor ='Tina Fey'
GROUP by actor,quality_class,streak_identifier

INSERT INTO richiesingh.actors_history_scd
-- get is active and quality class from previous year using LAG for a given actor
with lagged as(
select actor, CASE WHEN is_active THEN 1 ELSE 0 END as is_active,
      CASE WHEN  LAG(is_active) OVER(PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END as is_active_last_year,
quality_class,
LAG(quality_class) OVER(PARTITION BY actor ORDER BY current_year) as quality_class_last_year, current_year
from actors 
WHERE current_year <= 2020
)
--find the streak to see when the status changed
, streaked as (
select *,
    SUM(CASE WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year
    THEN 1 ELSE 0 END) OVER(PARTITION BY actor ORDER BY current_year) as streak_identifier
from lagged
)

-- find the start and end date
SELECT actor,quality_class,MAX(is_active)=1 as is_active,
      min(current_year) start_date,
      max(current_year) end_date,
      2020 as current_year
from streaked
--WHERE actor ='Tina Fey'
GROUP by actor,quality_class,streak_identifier
