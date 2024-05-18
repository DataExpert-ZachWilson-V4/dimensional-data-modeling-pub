INSERT INTO
  actors WITH last_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1999
  ),
  this_year AS (
    SELECT
      *
    FROM
      bootcamp.actor_films
    WHERE
      year = 2000
  ),
  actor_data AS (
    SELECT
      actor_id,
      AVG(rating) AS avg_rating,
      ARRAY_AGG(
        ROW(year, film, votes, rating, film_id)
      ) as films
    FROM
      bootcamp.actor_films
    WHERE
      year = 2000
    GROUP BY
      actor_id
  )
SELECT
  DISTINCT COALESCE(ty.actor, ly.actor) as actor,
  COALESCE(ty.actor_id, ly.actor_id) as actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL
    AND ly.films IS NOT NULL THEN ad.films || ly.films
    WHEN ty.year IS NOT NULL
    AND ly.films IS NULL THEN ad.films
  END as films,
  CASE
    WHEN ad.avg_rating > 8 THEN 'star'
    WHEN ad.avg_rating > 7
    AND ad.avg_rating <= 8 THEN 'good'
    WHEN ad.avg_rating > 6
    AND ad.avg_rating <= 7 THEN 'average'
    WHEN ad.avg_rating <= 6 THEN 'bad'
  END as quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) as current_year
FROM
  last_year ly FULL
  OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
  AND ty.actor = ly.actor
  LEFT JOIN actor_data ad ON ad.actor_id = COALESCE(ty.actor_id, ly.actor_id)