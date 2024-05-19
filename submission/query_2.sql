INSERT INTO richiesingh.actors 
--actors from last year
WITH last_year as (
SELECT * FROM richiesingh.actors
where current_year = 2019 
)
--actors from current year 
, this_year as (
SELECT actor,actor_id,
   ARRAY_AGG(
      ROW(
        film,
        votes,
        rating,
        film_id,
        year
      )
    ) as film,
   ---calculate quality_class
      CASE WHEN avg(rating) > 8 THEN 'star'
           WHEN avg(rating) > 7 and avg(rating) <=8 THEN 'good' 
           WHEN avg(rating) > 6 and avg(rating) <= 7 THEN 'average'
           WHEN avg(rating) <= 6 THEN 'bad'
 END as quality_class,
    max(year) as year
 FROM bootcamp.actor_films 
where year = 2020
GROUP BY actor,actor_id
)

select 
  COALESCE(ls.actor,ts.actor) as actor,
  COALESCE(ls.actor_id,ts.actor_id) as actor_id,
 CASE
    WHEN ts.film IS NULL THEN ls.films
    WHEN ts.film IS NOT NULL AND ls.films IS NULL THEN  ts.film
    WHEN ts.film IS NOT NULL AND ls.films IS NOT NULL THEN ls.films || ts.film END AS films,
  CASE WHEN ts.quality_class IS NULL THEN ls.quality_class 
       ELSE ts.quality_class
  END as quality_class,
  CASE WHEN ts.year IS NOT NULL THEN TRUE ELSE FALSE END as is_active,
  COALESCE(ts.year,ls.current_year+1) as current_year
from last_year ls 
--FULL JOIN to catch all the details from past and current year using COAELSCE
FULL OUTER JOIN this_year ts
  on ls.actor_id=ts.actor_id
