-- query to populate 'actors' table, one year at a time.
-- 'current_year' within last_year CTE, and 'year' within this_year CTE, have to be increased by 1 with each pass.

INSERT INTO steve_hut.actors
-- Start by querying last_year within the actors table. This_year will then be JOINED on last_year to incrementally build the cumulative table year by year.
WITH last_year AS (
  SELECT
      *
  FROM steve_hut.actors
  WHERE current_year = 1913 -- increment by 1 with each pass.
),
-- Need and ARRAY_AGG() since actors can have more than one film in a given year.
this_year AS (
  SELECT
    actor,
    actor_id,
    ARRAY_AGG(
      ROW(
        film,
        votes,
        rating,
        film_id
        )
      ) as films,
    CASE
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
        WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
        WHEN AVG(rating)  <= 6 THEN 'bad'
    END as quality_class,
    year
  FROM steve_hut.actor_films
  WHERE year = 1914 -- increment by 1 with each pass.
  GROUP BY actor, actor_id, year
)
-- insert cumulative results after JOINING last_year and this_year
  SELECT
      COALESCE(ly.actor, ty.actor) AS actor,
      COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
      CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY_CONCAT(ty.films, ly.films)
      END AS films,
      COALESCE(ty.quality_class, ly.quality_class) as quality_class,
      ty.actor_id IS NOT NULL AS is_active,
      COALESCE(ty.year, ly.current_year + 1) AS current_year
  FROM last_year ly
  FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
