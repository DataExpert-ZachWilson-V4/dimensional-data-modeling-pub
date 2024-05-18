--query_2

INSERT INTO actors
WITH last_year AS (
  SELECT * from actors
  WHERE current_year = 2000
),
this_year AS (
  SELECT 
    actor,
    actor_id,
    ARRAY_AGG(ROW(
        film,
        votes,
        rating,
        film_id,
        year
        )) as films,
   SUM(votes*rating)/SUM(votes) as avg_rating,
   year    
  FROM actor_films
  WHERE year = 2001
  group by actor, actor_id, year
)

SELECT 
  COALESCE(ly.actor, ty.actor) as actor,
  COALESCE(ly.actor_id, ty.actor_id) as actor_id,
 CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL 
            THEN (ty.films || ly.films)
    END AS films,
 CASE
     WHEN ty.avg_rating > 8 THEN 'star'
     WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
     WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
     ELSE 'bad'
  END AS quality_class, 
  CASE 
    WHEN ty.year is NOT NULL then TRUE
    ELSE FALSE
  END as is_active,
  COALESCE(ty.year, ly.current_year+1) as current_year
FROM last_year ly 
FULL OUTER JOIN this_year ty
ON ly.actor_id=ty.actor_id
