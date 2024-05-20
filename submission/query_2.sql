INSERT INTO devpatel18.actors
-- last year CTE
WITH last_year AS (
    SELECT *
    FROM devpatel18.actors
    WHERE current_year = 2010
),

-- this year CTE
this_year AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(
            ROW(
        year,
            film,
            votes,
            rating,
            film_id
            )
        ) AS films, -- aggregating all the films for a particular actor in a year
       SUM(votes * rating) / SUM(votes) as avg_rating,
       year
    FROM bootcamp.actor_films
    WHERE year = 2011
    GROUP BY actor, actor_id, year
)

-- CTD, coalescing unchanging values,full outer join to include new records, discontinued records, existing records are concatenated, records included from 2010 to 2014
SELECT 
 COALESCE(ly.actor, ty.actor) AS actor,
 COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
 CASE
  WHEN ty.films IS NULL THEN ly.films
  WHEN ly.films IS NULL THEN ty.films
 WHEN ly.films IS NOT NULL AND ty.films IS NOT NULL     
 THEN (ty.films || ly.films)
 END AS films,
 CASE
  WHEN ty.avg_rating > 8 THEN 'star'
  WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8  THEN 'good'
  WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7  THEN 'average'
  ELSE 'bad'
 END AS quality_class,
 CASE
  WHEN ty.year IS NOT NULL THEN true ELSE false
 END AS is_active,
 COALESCE(ty.year, ly.current_year + 1) AS current_year 
FROM this_year ty
FULL OUTER JOIN last_year ly
ON ty.actor_id = ly.actor_id
