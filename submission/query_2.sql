-- Insert data into the raniasalzahrani.actors table
INSERT INTO raniasalzahrani.actors (
  actor,
  actor_id,
  films,
  quality_class,
  is_active,
  current_year
)
-- Define a Common Table Expression (CTE) named yearly_actors
WITH yearly_actors AS (
  -- Select relevant columns and compute additional columns from the bootcamp.actor_films table
  SELECT
    actor,
    actor_id,
    film,
    votes,
    rating,
    film_id,
    YEAR,
    -- Determine the quality class of the actor's performance based on average rating per year
    CASE
    WHEN AVG(rating) OVER (PARTITION BY actor_id, YEAR) > 8 THEN 'star'
      WHEN AVG(rating) OVER (PARTITION BY actor_id, YEAR) > 7 THEN 'good'
      WHEN AVG(rating) OVER (PARTITION BY actor_id, YEAR) > 6 THEN 'average'
      ELSE 'bad'
    END AS quality_class,
    -- Determine if the actor is active by checking if the maximum year for the actor is the current row's year
    MAX(YEAR) OVER (PARTITION BY actor_id) = YEAR AS is_active
  FROM
    bootcamp.actor_films
)
-- Select data from the CTE to insert into the final table
SELECT
  actor,
  actor_id,
  -- Aggregate films information into an array of rows
  ARRAY_AGG(
    CAST(
      ROW(film, votes, rating, film_id) AS ROW(
        film VARCHAR,
        votes INTEGER,
        rating DOUBLE,
        film_id VARCHAR
      )
    )
  ) AS films,
  quality_class,
  is_active,
  YEAR AS current_year
FROM
  yearly_actors
GROUP BY
  actor,
  actor_id,
  quality_class,
  is_active,
  YEAR
