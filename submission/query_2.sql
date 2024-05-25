


-- Insert into the actors table
INSERT INTO akp389.actors
-- CTE 'last_year' retrieves data from the previous year for comparison and merging.
WITH last_year AS (
  SELECT *
  FROM akp389.actors
  WHERE current_year = 1999
),
-- CTE 'this_year' retrieves the current year's data from the bootcamp.actor_films dataset.
this_year AS (
  SELECT *
  FROM bootcamp.actor_films
  WHERE year = 2000
)
-- The main SELECT statement merges 'last_year' and 'this_year' data.
SELECT
  -- Coalescing actor names and IDs to ensure no null values.
  COALESCE(l.actor, t.actor) AS actor,
  COALESCE(l.actor_id, t.actor_id) AS actor_id,
  -- Merging film data from both years, adding new films to existing actor records.
  CASE
    WHEN l.films IS NULL THEN ARRAY_AGG(ROW(t.year, t.film, t.votes, t.rating, t.film_id))
    WHEN t.year IS NOT NULL THEN l.films || ARRAY_AGG(ROW(t.year, t.film, t.votes, t.rating, t.film_id))
    ELSE l.films
  END AS films,
  -- Recalculating the quality_class based on the latest year's average film ratings.
  CASE
    WHEN AVG(t.rating) IS NOT NULL THEN
      CASE
        WHEN AVG(t.rating) > 8 THEN 'star'
        WHEN AVG(t.rating) > 7 THEN 'good'
        WHEN AVG(t.rating) > 6 THEN 'average'
        ELSE 'bad'
      END
    ELSE l.quality_class
  END AS quality_class,
  -- Determining if the actor is currently active based on this year's data.
  CASE
    WHEN t.actor_id IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_active,
  -- Updating the current_year for each actor record.
  COALESCE(t.year, l.current_year + 1) AS current_year
FROM last_year l  
-- Using a FULL OUTER JOIN to ensure all records from both years are included.
FULL OUTER JOIN this_year t
ON l.actor_id = t.actor_id
-- Grouping by necessary fields to ensure correct aggregation and merging.
GROUP BY l.actor, t.actor, l.actor_id, t.actor_id, l.films, t.year, l.current_year, l.quality_class

  