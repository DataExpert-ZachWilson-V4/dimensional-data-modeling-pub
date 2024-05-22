INSERT INTO
  actors
WITH
  last_year_data AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1985
  ),
  this_year_data AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
      CASE
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating) > 7 THEN 'good'
        WHEN AVG(rating) > 6 THEN 'average'
        ELSE 'bad'
      END AS quality_class,
      actor_id IS NOT NULL AS is_active,
      YEAR AS current_year
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 1984
    GROUP BY
      actor,
      actor_id,
      YEAR
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
  -- This years' film is null, choose last year
    WHEN ty.films IS NULL THEN ly.films
    -- this years' film is not null but last years' is, choose current year only
    WHEN ty.films IS NOT NULL
    AND ly.films IS NULL THEN ty.films
    -- else, add to row
    WHEN ty.films IS NOT NULL
    AND ly.films IS NOT NULL THEN ly.films || ty.films
  END AS films,
  COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
  ty.is_active IS NOT NULL AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
  last_year_data AS ly
  -- Full outer join to get all actors from last year and this year
  FULL OUTER JOIN this_year_data AS ty ON ly.actor_id = ty.actor_id
