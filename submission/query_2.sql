-- Query populates the actors table one year at a time starting from 2018 to 2021
INSERT INTO
  RaviT.actors
-- Common Table Expression (CTE) to get data from the previous year
WITH
  last_year AS (
    SELECT
      * -- Select all columns from the actors table
    FROM
      RaviT.actors
    WHERE
      current_year = 2017
  ),
  -- CTE to get data for the current year (2018)
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
      ) AS films, -- Aggregate into an array of films
      AVG(rating) AS avg_rating, -- Calculate average rating
      year -- Select year
    FROM
      bootcamp.actor_films 
    WHERE
      year = 2018 
    GROUP BY
      actor, -- Group by actor name
      actor_id, -- Group by actor ID
      year -- Group by year
  )
-- Select combined results from the CTEs
SELECT
  COALESCE(ly.actor, ty.actor) AS actor, -- Use actor from 'this_year' if it exists, otherwise use from 'last_year'
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id, -- Use actor_id from 'this_year' if it exists, otherwise use from 'last_year'
  CASE
    WHEN ty.year IS NULL THEN ly.films -- If 'this_year' is null, use films from 'last_year'
    WHEN ty.year IS NOT NULL
      AND ly.films IS NULL THEN ty.films -- If 'last_year' films is null, use films from 'this_year'
    WHEN ty.year IS NOT NULL
      AND ly.films IS NOT NULL THEN ty.films || ly.films -- If both are not null, concatenate films arrays
  END AS films,
  CASE
    WHEN ty.avg_rating IS NULL THEN ly.quality_class -- If 'this_year' avg_rating is null, use quality_class from 'last_year'
    ELSE
      CASE
        WHEN ty.avg_rating > 8 THEN 'star' -- If avg_rating > 8, classify as 'star'
        WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good' -- If avg_rating > 7 and <= 8, classify as 'good'
        WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average' -- If avg_rating > 6 and <= 7, classify as 'average'
        WHEN ty.avg_rating <= 6 THEN 'bad' -- If avg_rating <= 6, classify as 'bad'
      END
  END AS quality_class,
  ty.year IS NOT NULL AS is_active, -- Check if 'this_year' is not null to determine active status
  COALESCE(ty.year, ly.current_year + 1) AS current_year -- Use 'this_year' year if not null, otherwise increment 'last_year' year by 1
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id -- Full outer join on actor_id