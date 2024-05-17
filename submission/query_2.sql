-- query_2

-- Insert data into the 'actors' table using data accumulated so far from previous years and the current year
INSERT INTO
  actors
WITH
  -- Subquery to select all actors from the previous years
  last_year AS (
    SELECT *
      FROM actors
     WHERE current_year = 1999
  ),
  -- Subquery to select actor information from the upcoming year 2000
  this_year AS (
    SELECT 
      actor,
      actor_id,
      ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
      AVG(rating) AS rating,
      year
    FROM actor_films
    WHERE year = 2000
    GROUP BY actor, actor_id, year
  )

-- Main query to insert data into the 'actors' table
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    -- Actor didn't star in a movie this year
    WHEN ty.year IS NULL
      THEN ly.films
    -- Actor started their career
    WHEN ty.year IS NOT NULL AND ly.films IS NULL
      THEN ty.films
    -- Actor continued their career
    WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL
      THEN ty.films || ly.films
  END AS films,
  CASE
    -- Didn't perform
    WHEN ty.rating IS NULL THEN NULL
    -- Star
    WHEN ty.rating > 8 THEN 'star'
    -- Good
    WHEN ty.rating > 7 THEN 'good'
    -- Average
    WHEN ty.rating > 6 THEN 'average'
    -- Bad
    ELSE 'bad'
  END AS quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year+1) AS current_year
  FROM last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor = ty.actor OR (ly.actor_id IS NULL AND ty.actor_id IS NOT NULL)
  
