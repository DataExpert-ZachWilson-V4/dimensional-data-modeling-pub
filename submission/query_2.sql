INSERT INTO jrsarrat.actors  --6,565 records inserted; 13,725 records inserted; 27,916 records inserted; 52,740 records inserted; 57,933 records inserted.
  
WITH actor_last_yr AS (
SELECT * FROM jrsarrat.actors
WHERE current_year = 2016 --I want to execute the same query each time by entering 2017 through 2020
),
actor_this_yr AS (
SELECT * FROM bootcamp.actor_films
WHERE YEAR = 2017 --I want to execute the same query each time by entering 2018 through 2021
)

SELECT 
  COALESCE(ly.actor, ty.actor) as actor,
  COALESCE(ly.actor_id, ty.actor_id) as actor_id,
  CASE
    WHEN ty.film IS NULL THEN ly.films
    WHEN ty.film IS NOT NULL
    AND ly.films IS NULL THEN ARRAY[ 
      ROW(
        ty.film,
        ty.YEAR,
        ty.votes,
        ty.rating,
        ty.film_id
      )
    ]
    WHEN ty.YEAR IS NOT NULL
    AND ly.current_year IS NOT NULL THEN ARRAY[
      ROW(
        ty.film,
        ty.YEAR,
        ty.votes,
        ty.rating,
        ty.film_id
      )
    ] || films
  END AS films,
  CASE
    WHEN rating > 8 THEN 'star'
    WHEN rating > 7 AND rating <= 8 THEN 'good'
    WHEN rating > 6 AND rating <= 7 THEN 'average'
    ELSE 'bad'
  END AS quality_class,
    CAST((CASE
    WHEN current_year = YEAR(CURRENT_DATE) THEN 1
    ELSE 0 
  END) AS boolean) AS is_active,
  COALESCE(ty.year, ly.current_year + 1) as current_year
FROM 
  actor_last_yr ly 
  FULL OUTER JOIN actor_this_yr ty 
    ON ly.actor = ty.actor
  
