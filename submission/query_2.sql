

INSERT INTO phabrahao.actors WITH last_year AS (
    SELECT *
    FROM phabrahao.actors
    WHERE current_year = 1913
  ),
  this_year AS (
    SELECT actor,
      actor_id,
      year,
      sum(votes * rating) / sum(votes) as avg_rating,
      array_agg(
        ROW(
          film,
          votes,
          rating,
          film_id
        )
      ) as film
    FROM bootcamp.actor_films
    WHERE year = 1914
    group by actor,
      actor_id,
      year
  )
SELECT COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.film IS NULL THEN ly.films
    WHEN ty.film IS NOT NULL
    AND ly.films IS NULL THEN ty.film
    WHEN ty.film IS NOT NULL
    AND ly.films IS NOT NULL THEN ty.film || ly.films
  END AS films,
  CASE
    WHEN avg_rating > 8 THEN 'star'
    WHEN avg_rating > 7 THEN 'good'
    WHEN avg_rating > 6 THEN 'average'
    WHEN avg_rating <= 6 THEN 'bad'
  END AS quality_class,
  ty.actor IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id