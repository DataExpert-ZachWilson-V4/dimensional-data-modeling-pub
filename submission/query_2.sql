/* Cumulative Table Computation query to populate actors table one at a time */

INSERT INTO supreethkabbin.actors
-- CTE for existing data in actors table from last year
WITH
  last_year AS (
    SELECT
      *
    FROM
      supreethkabbin.actors
    WHERE
      current_year = 1913
),
-- CTE for new data coming from actor_films for current year
this_year AS (
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
    )) as films,
    AVG(rating) as avg_rating,
    MAX(year) as current_year
  FROM
    bootcamp.actor_films
  WHERE year = 1914
  GROUP BY
    actor, 
    actor_id, 
    year
)
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    -- Actor data isn't present in incoming data for this year
    WHEN ty.current_year IS NULL THEN ly.films
    -- New actor data coming in 
    WHEN ty.current_year IS NOT NULL AND ly.films IS NULL THEN ty.films
    -- Existing actor with new data for this year
    WHEN ty.current_year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  CASE 
    WHEN ty.avg_rating is NOT NULL THEN (
      CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
      END 
    )
    ELSE ly.quality_class
  END as quality_class,
  ty.current_year IS NOT NULL AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) as current_year
FROM  last_year ly
FULL OUTER JOIN this_year ty 
  ON ly.actor_id = ty.actor_id