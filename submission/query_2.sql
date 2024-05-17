INSERT INTO actors
WITH last_year AS (
  SELECT *
  FROM actors
  WHERE current_year = 2015
),
this_year AS (
  SELECT
    actor,
    actor_id,
    year,
    ARRAY_AGG(ROW(film, film_id, year, votes, rating)) AS films,
    AVG(rating) AS avg_rating
  FROM bootcamp.actor_films
  WHERE year = 2016
  GROUP BY 
    actor,
    actor_id,
    year
)
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.avg_rating > 8 THEN 'star'
    WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
    WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
    WHEN ty.avg_rating <= 6 THEN 'bad'
  END AS quality_class,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id

-- Testing the output table
-- SELECT *
-- FROM actors
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year
