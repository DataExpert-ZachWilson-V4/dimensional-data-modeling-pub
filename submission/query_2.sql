--Ideally would parameterize the current year rather than hardcoding. Airflow is a potential solution for this.
--Also we should add a DELETE statement on existing records for the provided current_year so that the process is idempotent.
INSERT INTO billyswitzer.actors
WITH last_year AS (
  SELECT actor,
    actor_id,
    films,
    quality_class,
    is_active,
    current_year
  FROM billyswitzer.actors
  where current_year = 1918
), current_year_temp AS (
--Compute average_rating first, then use the result in the current_year CTE to calculate quality_class
  SELECT actor,
    actor_id,
    year,
    ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
    AVG(rating) AS average_rating
  FROM bootcamp.actor_films
  WHERE year = 1919
  GROUP BY actor,
    actor_id,
    year
), current_year AS (
  SELECT actor,
    actor_id,
    year,
    films,
    CASE WHEN average_rating > 8 THEN 'star'
      WHEN average_rating > 7 THEN 'good'
      WHEN average_rating > 6 THEN 'average'
      ELSE 'bad' END AS quality_class
  FROM current_year_temp
)
SELECT COALESCE(ly.actor, cy.actor) as actor,
  COALESCE(ly.actor_id, cy.actor_id) as actor_id,
  CASE WHEN ly.films IS NOT NULL AND cy.films IS NOT NULL THEN cy.films || ly.films
    WHEN ly.films IS NULL THEN cy.films
    WHEN cy.films IS NULL THEN ly.films END AS films,
  COALESCE(cy.quality_class, ly.quality_class) as quality_class,
  cy.actor_id IS NOT NULL as is_active,
  COALESCE(cy.year, ly.current_year + 1) as current_year
FROM last_year ly
  FULL OUTER JOIN current_year cy on ly.actor_id = cy.actor_id
