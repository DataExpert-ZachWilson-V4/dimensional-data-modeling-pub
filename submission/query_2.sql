INSERT INTO actors
-- Add a CTE for the previous year's data
WITH last_year AS (
  SELECT
    actor,
    actor_id,
    films,
    quality_class,
    current_year
  FROM
    actors
  WHERE
    current_year =1993
),
this_year AS (
  SELECT
    actor,
    actor_id,
    ARRAY_AGG(ROW(film, votes, rating, film_id, year)) AS films,
    YEAR
  FROM
    bootcamp.actor_films
  WHERE
    YEAR = 1994
  GROUP BY
    actor,
    actor_id,
    YEAR
)
  SELECT
  COALESCE(ly.actor, ty.actor ) AS actor ,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films ---keep existing films only if no data for this year
    WHEN ty.year IS NOT NULL
    AND ly.films IS NULL THEN ty.films ---add films for new actor first year
    WHEN ty.year IS NOT NULL
    AND ly.films IS NOT NULL THEN ly.films || ty.films ---append current year films to existing films
  END AS films,
  CASE
    WHEN ty.year IS NULL THEN ly.quality_class
    ---REDUCE fuction to sum up and count ratings(3rd element in the films row) for this year's movies and then get average for quality_class rating
    WHEN ty.year IS NOT NULL THEN REDUCE(
      ty.films,
      CAST(ROW(0.0, 0) AS ROW(SUM DOUBLE, COUNT INTEGER)),
      (s, r) -> CAST(
        ROW(r[3] + s.sum, s.count + 1) AS ROW(SUM DOUBLE, COUNT INTEGER)
      ),
      s -> CASE
        WHEN s.sum / s.count > 8 THEN 'star'
        WHEN s.sum / s.count > 7 THEN 'good'
        WHEN s.sum / s.count > 6 THEN 'average'
        ELSE 'bad'
      END
    )
  END AS quality_class, ---categorical value based on average rating for films released in the current year
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id  = ty.actor_id
