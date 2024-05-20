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
  year
  FROM steve_hut.actor_films
  WHERE year = 1914 -- increment by 1 with each pass.
  GROUP BY actor, actor_id, year
),
-- Create cumulative results after JOINING last_year and this_year
cumulative_this_year AS (
  SELECT
      COALESCE(ly.actor, ty.actor) AS actor,
      COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
      CASE
        WHEN ty.films IS NULL THEN CAST(ly.films AS ARRAY(ROW(film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR)))
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN CAST(ty.films AS ARRAY(ROW(film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR)))
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN CAST(ty.films || ly.films AS ARRAY(ROW(film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR)))
      END AS films,
      ty.year IS NOT NULL AS is_active,
      COALESCE(ty.year, ly.current_year + 1) AS current_year
  FROM last_year ly
  FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
)
-- Insert results into actors table, after calculating quality_class (average rating) based on cumulative_this_year films.
SELECT
  cty.actor,
  cty.actor_id,
  cty.films,
  CASE
    WHEN REDUCE(cty.films, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s, f) ->
      CAST(ROW(f.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
      s -> IF(s.count = 0, NULL, s.sum / s.count)) > 8 THEN 'star'
    WHEN REDUCE(cty.films, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s, f) ->
      CAST(ROW(f.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
      s -> IF(s.count = 0, NULL, s.sum / s.count)) > 7 THEN 'good'
    WHEN REDUCE(cty.films, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s, f) ->
      CAST(ROW(f.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
      s -> IF(s.count = 0, NULL, s.sum / s.count)) > 6 THEN 'average'
    WHEN REDUCE(cty.films, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s, f) ->
      CAST(ROW(f.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
      s -> IF(s.count = 0, NULL, s.sum / s.count)) <= 6 THEN 'bad'
  END AS quality_class,
  cty.is_active,
  cty.current_year
FROM cumulative_this_year cty

