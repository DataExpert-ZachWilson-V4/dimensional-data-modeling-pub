INSERT INTO ebrunt.actors WITH previous_year AS (
  SELECT 
    * 
  FROM 
    ebrunt.actors 
  WHERE 
    current_year = 1955
), 
current_year AS (
  SELECT 
    actor, 
    actor_id, 
    year, 
    array_agg(
      CAST(
        ROW(year, film, votes, rating, film_id) AS ROW(
          year INTEGER, film VARCHAR, votes INTEGER, 
          rating DOUBLE, film_id VARCHAR
        )
      )
    ) as films 
  FROM 
    bootcamp.actor_films 
  WHERE 
    year = 1956 
  GROUP BY 
    1, 
    2, 
    3
), 
cumlative_table AS (
  SELECT 
    COALESCE(py.actor, cy.actor) as actor, 
    COALESCE(py.actor_id, cy.actor_id) as actor_id, 
    CASE WHEN py.films IS NULL THEN cy.films WHEN cy.films IS NULL THEN py.films ELSE py.films || cy.films END as combined_films, 
    REDUCE(
      cy.films, 
      0, 
      (s, x) -> s + x.rating, 
      s -> s
    ) / REDUCE(
      cy.films, 
      0, 
      (s, x) -> s + 1, 
      s -> s
    ) as recent_avg_rating, 
    py.quality_class, 
    cy.year IS NOT NULL as is_active, 
    COALESCE(cy.year, py.current_year + 1) as current_year 
  FROM 
    previous_year as py FULL 
    OUTER JOIN current_year as cy ON py.actor_id = cy.actor_id
) 
SELECT 
  actor, 
  actor_id, 
  combined_films as films, 
  CASE WHEN recent_avg_rating IS NULL THEN quality_class WHEN recent_avg_rating > 8 THEN 'star' WHEN recent_avg_rating <= 8 
  AND recent_avg_rating > 7 THEN 'good' WHEN recent_avg_rating <= 7 
  AND recent_avg_rating > 6 THEN 'average' ELSE 'bad' END as quality_class, 
  is_active, 
  current_year 
FROM 
  cumlative_table
