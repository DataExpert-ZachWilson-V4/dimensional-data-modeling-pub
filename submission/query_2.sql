-- CUMULATIVE TABLE [INCREMENTAL LOAD] => Below query populates the actors table one year at a time
-- We need to aggregate actor_films table to arrive at one row per actor by aggregating all movies

INSERT INTO tharwaninitin.actors
WITH
  last_year AS (
    SELECT *
    FROM tharwaninitin.actors
    WHERE current_year = 1913
  ),
  this_year AS (
    SELECT actor, actor_id, year,
      ARRAY_AGG( ROW(film, votes, rating, film_id) ) films,
      CASE WHEN avg(rating) > 8 THEN 'star'
        WHEN avg(rating) > 7 and avg(rating) <=8 THEN 'good'
        WHEN avg(rating) > 6 and avg(rating) <= 7 THEN 'average'
        WHEN avg(rating) <= 6 THEN 'bad'
      END as quality_class
    FROM bootcamp.actor_films
    WHERE year = 1914
    GROUP BY actor, actor_id, year
  )
SELECT
  COALESCE(ty.actor, ly.actor) AS actor,
  COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
  CASE
    WHEN ty.films IS NULL THEN ly.films
    WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
    WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id