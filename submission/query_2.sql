INSERT INTO ningde95.actors
-- Define a Common Table Expression (CTE) named 'last_year'
WITH 
  last_year AS (
    SELECT
      *
    FROM
      ningde95.actors
    WHERE
      current_year = 2007
  ),
  -- Select all columns from the 'actors' table in the 'ningde95' schema and Filter the rows to include only those where 'current_year' equals 2007
 -- Define a Common Table Expression (CTE) named 'this_year'
 this_year AS (
  -- Select specific columns from the 'actor_films' table
  SELECT
    actor,  -- Select the 'actor' column
    actor_id,  -- Select the 'actor_id' column
    MAX(year) AS year,  -- Select the maximum 'year' and alias it as 'year'
    -- Aggregate 'film', 'votes', 'rating', and 'film_id' into an array and alias it as 'films'
    ARRAY_AGG(row(film, votes, rating, film_id)) AS films,
    -- Calculate the average 'rating' and alias it as 'AVG_rating'
    AVG(rating) AS AVG_rating
  FROM
    bootcamp.actor_films  -- From the 'actor_films' table in the 'bootcamp' schema
  WHERE
    year = 2008  -- Where 'year' is 2008
  GROUP BY
    actor,  -- Group by 'actor'
    actor_id  -- Group by 'actor_id'
)
-- Select the final result set with various calculations and transformations
SELECT
  -- Coalesce returns the first non-null value between 'ls.actor' and 'ts.actor', alias it as 'actor'
  COALESCE(ls.actor, ts.actor) AS actor, 
  -- Coalesce returns the first non-null value between 'ls.actor_id' and 'ts.actor_id', alias it as 'actor_id'
  COALESCE(ls.actor_id, ts.actor_id) AS actor_id, 
  -- Use a CASE statement to determine the 'films' array based on the presence of values in 'ls.films' and 'ts.films'
  CASE 
    WHEN ls.films IS NULL 
      THEN ts.films
    WHEN ls.films IS NOT NULL AND ts.year IS NOT NULL
      THEN ts.films || ls.films
    WHEN ls.films IS NOT NULL AND ts.year IS NULL
      THEN ls.films
  END AS films,
  -- Use a CASE statement to classify the quality based on 'ts.AVG_rating'
  CASE
    WHEN ts.AVG_rating > 8 THEN 'star'
    WHEN ts.AVG_rating > 7 AND ts.AVG_rating <= 8 THEN 'good'
    WHEN ts.AVG_rating > 6 AND ts.AVG_rating <= 7 THEN 'average'
    WHEN ts.AVG_rating <= 6 THEN 'bad'
  END AS quality_class,
  -- Check if 'ts.year' is not null and alias it as 'is_active' (boolean)
  ts.year IS NOT NULL AS is_active,
  -- Coalesce returns the first non-null value between 'ls.current_year + 1' and 'ts.year', alias it as 'current_year'
  COALESCE(ls.current_year + 1, ts.year) AS current_year
FROM
  -- Perform a FULL OUTER JOIN between 'last_year' (ls) and 'this_year' (ts) on the 'actor' field
  last_year ls
  FULL OUTER JOIN this_year ts ON ls.actor = ts.actor
