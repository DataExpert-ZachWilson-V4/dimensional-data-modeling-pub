-- change the current_year and year by current_year + 1 to get data populated into satheeshkandula0351185.actors table one year at a time

INSERT INTO satheeshkandula0351185.actors
WITH last_year AS (
  SELECT 
  *
  FROM satheeshkandula0351185.actors
  WHERE current_year = 1913 -- no movies in this year
),
this_year AS (
  SELECT 
  *
  FROM bootcamp.actor_films
  WHERE year = 1914 -- initial records
)

SELECT 
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.film IS NULL THEN ly.films
    WHEN ty.film IS NOT NULL AND ly.films IS NULL THEN ARRAY[
      ROW(
        ty.film,
        ty.votes,
        ty.rating,
        ty.film_id
      )
    ]
    WHEN ty.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[
      ROW(
        ty.film,
        ty.votes,
        ty.rating,
        ty.film_id
      )
    ] || ly.films
  END AS films,
  CASE
    WHEN AVG(ty.rating) OVER (PARTITION BY COALESCE(ly.actor, ty.actor)) > 8 THEN 'star'
    WHEN AVG(ty.rating) OVER (PARTITION BY COALESCE(ly.actor, ty.actor)) > 7 THEN 'good'
    WHEN AVG(ty.rating) OVER (PARTITION BY COALESCE(ly.actor, ty.actor)) > 6 THEN 'average'    
    ELSE 'bad'
  END AS quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id