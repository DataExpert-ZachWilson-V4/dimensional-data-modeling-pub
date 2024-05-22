Insert INTO actors

WITH
  last_year AS (
    SELECT
      *
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 1924
  ),
  this_season AS (
    SELECT
      *
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 1925
  )
  

SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id, 
  ARRAY[
      ROW(
        ty.film,
        ty.votes,
        ty.rating,
        ty.film_id
      
      )
    ]
    as films,
  ty.year is not null as is_active,
  ty.year as current_year
  
  
from last_year ly
  FULL OUTER JOIN this_season ty ON ly.actor = ty.actor
