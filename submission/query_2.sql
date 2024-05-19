insert into ivomuk37854.actors
WITH previous_year AS (
select * from ivomuk37854.actors
 where current_year = 1960
),
this_year AS (
 select   
    actor,
    actor_id,
    ARRAY_AGG(
      ROW(
       film,
        film_id,
        votes,
        rating,        
        year
      )
    ) AS films,
    AVG(rating) AS avg_rating,
    year
  FROM
    bootcamp.actor_films
  WHERE
    rating is not null
    and year = 1961
  GROUP BY
    actor,
    actor_id,
    year 
)
select  
  COALESCE(py.actor, ty.actor) AS actor,
  COALESCE(py.actor_id, ty.actor_id) AS actor_id,
CASE 
when py.films is null then ty.films
when ty.films is null then py.films 
else ty.films || py.films
end as films,
--WHEN ty.films IS NULL THEN py.films
--WHEN ty.films IS NOT NULL and py.films IS NULL
--THEN ty.films
--WHEN ty.films IS NOT NULL and py.films IS NOT --NULL THEN ty.films || py.films
--END AS films,
CASE WHEN avg_rating > 8 THEN 'star'
     WHEN avg_rating > 7 THEN 'good'
     WHEN avg_rating > 6 THEN 'average'
     ELSE 'bad'
END AS quality_class,
ty.year IS NOT NULL AS is_active, 
COALESCE(ty.year, py.current_year + 1) AS current_year
from previous_year py
 FULL OUTER JOIN this_year ty
 ON py.actor_id = ty.actor_id
