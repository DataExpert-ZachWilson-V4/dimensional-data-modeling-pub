INSERT INTO
  alia.actors
WITH
  last_year AS (
    SELECT
      actor,
      actor_id,
      films,
      quality_class,
      is_active,
      current_year
    FROM
      alia.actors
    WHERE
      current_year = 2001
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      year,
      array_agg (ROW (year, film, votes, rating, film_id)) films,
      CASE
        WHEN AVG(rating) > 8.0 THEN 'star'
        WHEN AVG(rating) > 7.0
        AND AVG(rating) <= 8.0 THEN 'good'
        WHEN AVG(rating) > 6.0
        AND AVG(rating) <= 7.0 THEN 'average'
        WHEN AVG(rating) <= 6.0 THEN 'bad'
      END as quality_class
    FROM
      bootcamp.actor_films
    WHERE
      year = 2002
    GROUP BY
      1,
      2,
      3
  )
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
  COALESCE(ty.quality_class, ly.quality_class) quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly -- ly = last year
  FULL OUTER JOIN this_year ty --  ty = this year
  ON ly.actor_id = ty.actor_id