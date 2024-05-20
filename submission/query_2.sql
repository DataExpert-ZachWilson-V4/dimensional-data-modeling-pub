INSERT INTO
  sravan.actors (
    actor,
    actor_id,
    films,
    quality_class,
    is_active,
    current_year
  )
WITH
  last_year AS (
    SELECT
      *
    FROM
      sravan.actors
    WHERE
      current_year = 2020
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(
        CAST(
          ROW(film, votes, rating, film_id) AS ROW(
            film VARCHAR,
            votes INTEGER,
            rating DOUBLE,
            film_id VARCHAR
          )
        )
      ) AS films,
      YEAR
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 2021
    GROUP BY
      actor,
      actor_id,
      YEAR
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, ty.actor) AS actor,
      COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
      CASE
        WHEN ty.year IS NULL THEN ly.films
        WHEN ty.year IS NOT NULL
        AND ly.current_year IS NULL THEN ty.films
        WHEN ty.year IS NOT NULL
        AND ly.current_year IS NOT NULL THEN ty.films || ly.films
      END AS films,
      NULL AS quality_class,
      ty.year IS NOT NULL AS is_active,
      COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM
      last_year ly
      FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
  )
SELECT
  actor,
  actor_id,
  films,
  quality_class,
  is_active,
  current_year
FROM
  (
    SELECT
      actor,
      actor_id,
      films,
      CASE
        WHEN AVG(t.rating) > 8 THEN 'star'
        WHEN AVG(t.rating) > 7
        AND AVG(t.rating) <= 8 THEN 'good'
        WHEN AVG(t.rating) > 6
        AND AVG(t.rating) <= 7 THEN 'average'
        WHEN AVG(t.rating) <= 6 THEN 'bad'
      END AS quality_class,
      is_active,
      current_year
    FROM
      combined,
      UNNEST (films) AS t (film, votes, rating, film_id)
    GROUP BY
      actor,
      actor_id,
      films,
      is_active,
      current_year
  ) subquery
