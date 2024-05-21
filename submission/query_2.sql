WITH
  last_year AS (
    SELECT
      *
    FROM
      denzelbrown.actors
    WHERE
      current_year = 1913
  ),
  this_year AS (
    SELECT
      af.actor,
      af.actor_id,
      ARRAY_AGG(ROW (af.film, af.votes, af.rating, af.film_id)) AS films,
      af.year,
      CASE
        WHEN AVG(af.rating) > 8 THEN 'star'
        WHEN AVG(af.rating) > 7
        AND AVG(af.rating) <= 8 THEN 'good'
        WHEN AVG(af.rating) > 6
        AND AVG(af.rating) <= 7 THEN 'average'
        ELSE 'bad'
      END AS quality_class,
      af.year IS NOT NULL AS is_active
    FROM
      bootcamp.actor_films af
    WHERE
      YEAR = 1914
    GROUP BY
      1,
      2,
      4,
      6
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actorid, ty.actor_id) AS actorid,
  CASE
    WHEN ty.films IS NULL THEN ly.films
    WHEN ty.films IS NOT NULL
    AND ly.films IS NULL THEN ty.films
    WHEN ty.films IS NOT NULL
    AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  COALESCE(ly.quality_class, ty.quality_class) AS quality_class,
  CASE
    WHEN ty.films IS NOT NULL THEN TRUE ELSE FALSE END AS is_active
 ,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actorid = ty.actor_id
