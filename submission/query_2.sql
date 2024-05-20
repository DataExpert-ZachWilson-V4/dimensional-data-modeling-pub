-- This query inserts actor data into the vpsjul8468082.actors table.
-- It merges actors' data from the previous year with the current year's data.
-- The query combines film details, updates the quality class, and marks active actors based on the current year's film data.

INSERT INTO vpsjul8468082.actors
-- Actors from the last year
WITH last_year AS (
  SELECT * FROM vpsjul8468082.actors
  WHERE current_year = 2020
),
-- Actors from the current year
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
    ) AS films,
    -- Calculate quality_class
    CASE 
      WHEN AVG(rating) > 8 THEN 'star'
      WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
      WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
      WHEN AVG(rating) <= 6 THEN 'bad'
    END AS quality_class,
    max(year) as year
  FROM bootcamp.actor_films 
  WHERE year = 2021
  GROUP BY actor, actor_id
)
SELECT 
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.films IS NULL THEN ly.films
    WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
    WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ly.films || ty.films
  END AS films,
  CASE 
    WHEN ty.quality_class IS NULL THEN ly.quality_class 
    ELSE ty.quality_class
  END AS quality_class,
  CASE 
    WHEN ty.year IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly 
-- FULL JOIN to catch all the details from the past and current year using COALESCE
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id
