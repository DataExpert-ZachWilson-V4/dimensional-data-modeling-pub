INSERT INTO actors
-- CTE to select actors from previously aggregated data in the previous year
WITH last_year AS (
  SELECT *
  FROM actors
  WHERE current_year = 2015
),
-- CTE to select and aggregate films for actors from raw data in the current year
this_year AS (
  SELECT
    actor,
    actor_id,
    year,
    -- Aggregate films into an array of rows with film details
    ARRAY_AGG(ROW(film, film_id, year, votes, rating)) AS films,
    -- Classify the quality of the actor based on average film rating
   CASE
      WHEN AVG(rating) > 8 THEN 'star'
      WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
      WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
      WHEN AVG(rating) <= 6 THEN 'bad'
    END AS quality_class
  FROM bootcamp.actor_films
  WHERE year = 2016
  GROUP BY 
    actor,
    actor_id,
    year
)
-- Select and combine data from both CTEs
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,  -- Use the actor name from last_year or this_year
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,  -- Use the actor ID from last_year or this_year
  COALESCE(ly.quality_class, ty.quality_class) AS quality_class,  -- Use the quality class from last_year or this_year
  -- Combine films from both years if available
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ly.films || ty.films
  END AS films,
  ty.year IS NOT NULL AS is_active,  -- Mark actor as active if they have films this year
  COALESCE(ty.year, ly.current_year + 1) AS current_year  -- Determine the current year for the actor
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id

-- Testing the output table
-- SELECT *
-- FROM actors
-- WHERE actor IN ('Adrienne Barbeau', 'Antonio Banderas', 'Brad Pitt', 'Chris Evans')
-- ORDER BY actor, current_year
