-- min year = 1914, max year = 2021
-- max number of movies per actor in 2013 by Eric Roberts

INSERT INTO ykshon52797255.actors

/*-----------------------------------------------------------------------------------------------------------
Create CTEs for the query
  last_film: grabs all data from the SCD table in previous year from the current year
  current_film: grabs all data from the SCD table in current year
    - creates an array films that combines all film, votes, rating, film_id during that year by the actor
*/------------------------------------------------------------------------------------------------------------
WITH
  last_film AS (
    SELECT
      *
    FROM
      ykshon52797255.actors
    WHERE
      current_year = 2023 -- CHANGE THIS VALUE
  ),
  current_film AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(CAST(ROW(year, film, votes, rating, film_id) AS 
                     ROW(year INTEGER, film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR))) AS films,
      year
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 2024 -- CHANGE THIS VALUE
    GROUP BY actor,actor_id, year
  )

/*-----------------------------------------------------------------------------------------------------------
query that populates the actors table one year at a time.
*/------------------------------------------------------------------------------------------------------------
  
SELECT
  COALESCE(lf.actor, cf.actor) AS actor,
  COALESCE(lf.actor_id, cf.actor_id) AS actor_id,
  CASE
    -- if current year's film is null, then grab last year's film
    WHEN cf.year IS NULL THEN lf.films
    -- if current year's film is not null last year's film is null then grab current year's film
    WHEN cf.year IS NOT NULL AND lf.films IS NULL THEN cf.films
    -- if both are null then concatenate current year's and last year's films
    WHEN cf.year IS NOT NULL AND lf.films IS NOT NULL THEN cf.films || lf.films
  END AS films,
  -- do calculations on quality_class
  CASE
    WHEN cf.year IS NULL THEN lf.quality_class
    WHEN cf.year IS NOT NULL THEN REDUCE( cf.films, CAST(ROW(0.0, 0) AS 
      ROW(sum DOUBLE, count INTEGER)), 
        (s, r) -> CAST(ROW(r.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
       s -> CASE WHEN s.sum / s.count > 8 THEN 'star' 
      WHEN s.sum/s.count > 7 and s.sum/s.count <= 8 THEN 'good'
      WHEN s.sum/s.count > 6 and s.sum/s.count <= 7 THEN 'average'
      else 'bad' 
    END
  ) 
  END AS quality_class,
  cf.year IS NOT NULL AS is_active,
  COALESCE(cf.year, lf.current_year + 1) AS current_year
FROM
  last_film lf
  FULL OUTER JOIN current_film cf ON lf.actor_id = cf.actor_id
