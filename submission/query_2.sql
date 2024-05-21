WITH
  last_year AS (
    SELECT
      *
    FROM
      ttian45759.actors
    WHERE
      current_year = 2000
  ),

  this_year AS (
    SELECT
      actor,
      actor_id,
      "year",
      ARRAY_AGG(
        ROW(
          film,
          votes,
          rating,
          film_id
        )
      ) AS films,
      CASE 
        WHEN avg(rating) > 8 THEN 'star'
        WHEN avg(rating) > 7 THEN 'good'
        WHEN avg(rating) > 6 THEN 'average'
        WHEN avg(rating) <= 6 THEN 'bad'
      END AS quality_class
    FROM
      bootcamp.actor_films
    WHERE
      "year" = 2001
    GROUP BY 1,2,3
  )

SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE 
    WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
    WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films 
END AS films,
  COALESCE(ly.quality_class,ty.quality_class) as quality_class,
  ty.films IS NOT NULL AS is_active,
  COALESCE(ly.current_year, ty."year") AS current_year
FROM
  last_year ly
FULL OUTER JOIN 
  this_year ty ON ly.actor = ty.actor
