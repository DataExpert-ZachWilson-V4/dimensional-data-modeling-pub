INSERT INTO whiskersreneewe.actors
WITH last_year_films AS (
    SELECT * FROM whiskersreneewe.actors
    WHERE current_year = 1920
),

this_year_avg AS (
    SELECT actor_id, year as this_year, AVG(rating) as avg_rating
    FROM bootcamp.actor_films
    WHERE year = 1921
    GROUP BY actor_id, year
),
this_year_films AS (
    SELECT actor, tya.actor_id, film,
    this_year, votes, rating, film_id, avg_rating
    FROM bootcamp.actor_films af
    JOIN this_year_avg tya ON af.actor_id = tya.actor_id
)

SELECT 
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN this_year IS NULL THEN ly.films
    WHEN ly.current_year IS NULL AND this_year IS NOT NULL THEN ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id
    )]
    WHEN ly.current_year IS NOT NULL AND this_year IS NOT NULL THEN ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id
    )] || ly.films
  END AS films,
  CASE 
    WHEN ty.avg_rating > 8 THEN 'star'
    WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
    WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
    WHEN ty.avg_rating <= 6 THEN 'bad'
  END AS quality_class,
  this_year is NOT NULL AS is_active,
  this_year AS current_year
FROM last_year_films as ly
FULL OUTER JOIN this_year_films ty
ON ly.actor_id = ty.actor_id



  

