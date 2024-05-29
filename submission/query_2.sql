INSERT INTO ChrisTaulbee.actors


WITH last_year as (
SELECT
  actor
  , actor_id
  , films
  , quality_class
  , is_active
  , current_year
FROM ChrisTaulbee.actors
WHERE current_year = 1914
)
,this_year as (
SELECT
  actor,
  actor_id,
  ARRAY_AGG(
    ROW(film, votes, rating, film_id, year)
  ) AS films,
  year as current_year,
  AVG(rating) as avg_rating
FROM
  bootcamp.actor_films
WHERE
  year = 1915
GROUP BY
  actor,
  actor_id,
  year )
SELECT
  COALESCE(ly.actor, ty.actor) as actor,
  COALESCE(ly.actor_id, ty.actor_id) as actor_id,
  CASE
    WHEN ly.films IS NULL THEN ty.films
    WHEN ty.films IS NULL THEN ly.films
    WHEN ly.films IS NOT NULL AND ty.films IS NOT NULL THEN (ly.films || ty.films)
  END AS films,
  CASE
    WHEN ty.avg_rating > 8 THEN 'star'
    WHEN ty.avg_rating > 7 THEN 'good'
    WHEN ty.avg_rating > 6 THEN 'average'
    WHEN ty.avg_rating <= 6 THEN 'bad'
  END as quality_class,
  CASE
    WHEN ty.films IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) as current_year
FROM
  last_year ly
FULL OUTER JOIN
  this_year ty ON ly.actor_id = ty.actor_id
  
  