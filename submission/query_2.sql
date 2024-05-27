-- Insert data into the actors table
INSERT INTO iliamokhtarian.actors (
  actor,
  actor_id,
  films,
  quality_class,
  is_active,
  current_year
)
WITH
  last_year AS (
    -- Fetch all actor info for the last year (1988)
    SELECT
      actor,
      actor_id,
      films,
      quality_class,
      current_year
    FROM
      iliamokhtarian.actors
    WHERE
      current_year = 1988
  ),
  this_year AS (
    -- Fetch all actor info for the current year (1989)
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
      ) AS films,
      AVG(rating) AS avg_rating,
      1989 AS current_year
    FROM
      bootcamp.actor_films
    WHERE
      year = 1989
    GROUP BY
      actor,
      actor_id
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE 
    WHEN ty.current_year IS NULL THEN ly.films
    WHEN ty.current_year IS NOT NULL AND ly.current_year IS NULL THEN ty.films
    WHEN ty.current_year IS NOT NULL AND ly.current_year IS NOT NULL THEN ty.films || ly.films
  END AS films,
  CASE
    WHEN ty.avg_rating IS NULL THEN ly.quality_class
    ELSE
      CASE
        WHEN ty.avg_rating <= 6 THEN 'bad'
        WHEN ty.avg_rating <= 7 THEN 'average'
        WHEN ty.avg_rating <= 8 THEN 'good'
        ELSE 'star'
      END
  END AS quality_class,
  ty.current_year IS NOT NULL AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
  last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
