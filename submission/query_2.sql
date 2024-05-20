-- Inserting data into the table "nancyatienno21998.actors"
INSERT INTO nancyatienno21998.actors

-- Using Common Table Expressions (CTEs) to organize the query
--- CTE to fetch data from last year (1921)
WITH last_year AS (
  SELECT * FROM nancyatienno21998.actors
  WHERE current_year = 1921
),
--- CTE to fetch data for the current year (1922) from another table named "bootcamp.actor_films"
this_year AS (
  SELECT * FROM bootcamp.actor_films
  WHERE year = 1922
)

-- Selecting data and combining last year and current year records
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,  -- Choose actor from last year if available, else from current year
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,  -- Choose actor_id from last year if available, else from current year
  CASE
    WHEN ty.year IS NULL THEN ly.films  -- If there are no films in the current year, use films from last year
    WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ARRAY[ -- If there are films in the current year but not in last year
     ROW(
     ty.year,
     ty.film,
     ty.votes,
     ty.rating,
     ty.film_id
     )
    ]
    WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ -- If there are films in both years, combine them
     ROW(
     ty.year,
     ty.film,
     ty.votes,
     ty.rating,
     ty.film_id
     )
    ] || ly.films
    END AS films,
  CASE
    WHEN AVG(ty.rating) OVER (PARTITION BY ty.actor_id) <= 6 THEN 'bad'  -- Quality classification based on average rating
    WHEN AVG(ty.rating) OVER (PARTITION BY ty.actor_id) <= 7 THEN 'average'
    WHEN AVG(ty.rating) OVER (PARTITION BY ty.actor_id) <= 8 THEN 'good'
    ELSE 'star'
  END AS quality_class,
  ty.year IS NOT NULL AS is_active,  -- Check if the actor is active in the current year
  COALESCE(ty.year, ly.current_year+1) AS current_year  -- Set the current year as the current year if available, else increment last year by 1
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id
