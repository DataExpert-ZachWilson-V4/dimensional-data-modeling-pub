-- Insert data into the saismail.actors table
INSERT INTO
  saismail.actors
  -- Common Table Expressions (CTEs) to define temporary datasets
  -- Select actors from last year
WITH
  last_year AS (
    SELECT
      *
    FROM
      saismail.actors
    WHERE
      current_year = 1919
  ),
  -- Select actors and films from this year
  this_year AS (
    SELECT
      *
    FROM
      bootcamp.actor_films
    WHERE
      "year" = 1920
  ),
  -- Create a temporary table with selected columns from this year
  temp_table AS (
    SELECT
      ty.actor AS actor,
      ty.actor_id AS actor_id,
      ROW(
        ty."year",
        ty.film,
        ty.votes,
        ty.rating,
        ty.film_id
      ) AS films,
      ty."year" AS current_year
    FROM
      this_year ty
  ),
  -- Create a temporary table with distinct actors and their films aggregated into an array
  temp_table2 AS (
    SELECT DISTINCT
      (actor) AS actor,
      actor_id,
      array_agg(films) OVER (
        PARTITION BY
          actor
      ) AS films,
      current_year
    FROM
      temp_table
  ),
  -- Calculate the average rating for each actor
  avg_rating AS (
    SELECT
      t.*,
      REDUCE(
        films,
        CAST(ROW(0.0, 0) AS ROW(SUM DOUBLE, COUNT INTEGER)),
        (s, x) -> CAST(
          ROW(x[4] + s.sum, s.count + 1) AS ROW(SUM DOUBLE, COUNT INTEGER)
        ),
        s -> IF(s.count = 0, NULL, s.sum / s.count)
      ) AS average_rating
    FROM
      temp_table2 AS t
  ),
  -- Determine the quality class based on the average rating
  quality_class AS (
    SELECT
      a.*,
      CASE
        WHEN a.average_rating > 8 THEN 'star'
        WHEN a.average_rating > 7
        AND a.average_rating <= 8 THEN 'good'
        WHEN a.average_rating > 6
        AND a.average_rating <= 7 THEN 'average'
        WHEN a.average_rating <= 6 THEN 'bad'
      END AS quality_class
    FROM
      avg_rating AS a
  )
  -- Main query to combine data from last year and this year, and determine if actors are active
SELECT
  COALESCE(ly.actor, q.actor) AS actor,
  COALESCE(ly.actor_id, q.actor_id) AS actor_id,
  CASE
    WHEN q."current_year" IS NULL THEN ly.films
    WHEN q."current_year" IS NOT NULL
    AND ly.films IS NULL THEN q.films
    WHEN q."current_year" IS NOT NULL
    AND ly.films IS NOT NULL THEN CONCAT(q.films, ly.films)
  END AS films,
  COALESCE(q.quality_class, ly.quality_class) AS quality_class,
  CASE
    WHEN ly.actor IS NULL THEN TRUE
    WHEN EXISTS (
      SELECT
        1
      WHERE
        ly.actor_id = q.actor_id
    ) THEN TRUE
    ELSE FALSE
  END AS is_active,
  COALESCE(q.current_year, ly.current_year + 1) AS current_year
FROM
  last_year AS ly
  FULL OUTER JOIN quality_class q ON ly.actor_id = q.actor_id
