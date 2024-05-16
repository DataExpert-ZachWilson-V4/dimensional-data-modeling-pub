-- Query to populate the actors data one year at a time
-- 

INSERT INTO pratzo.actors
WITH
  -- Using CTE to get actor info for last year 
  last_year AS(
    SELECT
      *
    FROM
      pratzo.actors
    WHERE
      current_year = 1975
  ),

  -- Using a CTE to get actor info for this year 
  this_year AS(
    SELECT
      actor,
      actor_id,
      -- Aggregate films into an array of structs
      ARRAY_AGG(ROW(film,votes,rating,film_id)) AS films,
      AVG(rating) AS avg_rating,
      MAX(year) AS current_year
    FROM
      bootcamp.actor_films
    WHERE
      year = 1976
    GROUP BY
      actor,
      actor_id
  )
-- Select data and combining them to insert into the actors table
SELECT
    -- Using coalesce to handle NULL values
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE 
      WHEN ty.current_year IS NULL 
        THEN ly.films
      WHEN ty.current_year IS NOT NULL
      AND ly.current_year IS NULL 
        THEN ty.films
      WHEN ty.current_year IS NOT NULL
      AND ly.current_year IS NOT NULL 
        THEN ty.films || ly.films
      END AS films,
    CASE
      WHEN ty.avg_rating IS NULL THEN ly.quality_class
      ELSE
        CASE
          WHEN ty.avg_rating  <= 6 THEN 'bad'
          WHEN ty.avg_rating <= 7 THEN 'average'
          WHEN ty.avg_rating <= 8 THEN 'good'
          ELSE 'star' END
      END AS quality_class,
      ty.current_year IS NOT NULL is_active,
      COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
  last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
