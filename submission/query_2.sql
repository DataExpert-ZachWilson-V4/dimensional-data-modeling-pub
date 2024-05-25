INSERT INTO actors
WITH
  last_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1913
  ),
  this_year AS (
    SELECT
    actor,
      actor_id,
      ARRAY_AGG((film, votes, rating, film_id, year)) films_array,
      avg(rating) avg_rating,
      year
    FROM
      bootcamp.actor_films
    WHERE
      year= 1914
    GROUP BY
      actor, actor_id, year
  )
SELECT
  coalesce(ly.actor, ty.actor) AS actor,
  coalesce(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL AND ly.films IS NULL THEN ARRAY[]
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films_array
    WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ly.films || ty.films_array
  END AS films,
  coalesce(CASE
    WHEN ty.avg_rating > 8 THEN 'star'
    WHEN ty.avg_rating > 7
    AND ty.avg_rating <= 8 THEN 'good'
    WHEN ty.avg_rating > 6
    AND ty.avg_rating <= 7 THEN 'average'
    WHEN ty.avg_rating <= 6 THEN 'bad'
      END, ly.quality_class) AS quality_class,
  ty.year IS NOT NULL AS is_active,
  coalesce(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
