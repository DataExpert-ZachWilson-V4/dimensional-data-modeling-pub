-- commulative table computation
-- pupulates the actors table one year at a time
-- the data ranges from 1914 - 2021 so we are going to take the last 5 years of data
-- assuming we want to have one row per actor


INSERT INTO actors

WITH
  last_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 2016
  ),
  this_year AS (
    SELECT
      year,
      actor,
      actor_id,
      ARRAY_AGG(
      ROW (
        film,
        votes,
        rating,
        film_id
      )
      ) AS film,
      AVG(rating) AS rating
      
    FROM
      bootcamp.actor_films
    WHERE
      year = 2017
    GROUP BY year, actor, actor_id
   
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
      WHEN ty.film IS NULL THEN ly.films
      WHEN ty.film IS NOT NULL
      AND ly.films IS NULL THEN ty.film
      WHEN ty.film IS NOT NULL
      AND ly.films IS NOT NULL THEN ty.film || ly.films
  END AS films,
  
  CASE
      WHEN ty.rating > 8 THEN 'star'
      WHEN ty.rating > 7 AND ty.rating <= 8 THEN 'good'
      WHEN ty.rating > 6 AND ty.rating <= 7 THEN 'average'
      WHEN ty.rating <= 6 THEN 'bad'
  END AS quality_class,

  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year +1 ) AS current_year

FROM
  last_year ly
  FULL OUTER JOIN this_year ty 
  ON ly.actor = ty.actor
