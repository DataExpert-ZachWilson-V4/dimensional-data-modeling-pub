--query_2: query that populates the actors table one year at a time
INSERT INTO
  aayushi.actors
WITH
  last_year AS (
    SELECT
      *
    FROM
      aayushi.actors
    WHERE
      current_year = 2007
  ), -- CTE to get the previous year's data from actors table	 
  this_year AS (
    SELECT
      *
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 2008
  ) -- CTE to get the latest year's data from source table

SELECT
-- using coalesce to avoid null values and combine common values
    coalesce(ly.actor_id, ty.actor_id) AS actor_id 
  , coalesce(ly.actor, ty.actor) AS actor
-- checking if current_years's data is null then using previous year's data for films. If previous year's data is null then current year's data is used to populate films. If both years data is available then using concat to append current year's data first
  , CASE
      WHEN ty.year IS NULL THEN ly.films
      WHEN ty.year IS NOT NULL
      AND ly.films IS NULL THEN ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id)]
      WHEN ty.year IS NOT NULL
      AND ly.films IS NOT NULL THEN ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id)] || ly.films
   END AS films
--categorizing quality class
  , CASE
     WHEN AVG(ty.rating) OVER (
       PARTITION BY
         ty.actor_id
     ) <= 6 THEN 'bad'
     WHEN AVG(ty.rating) OVER (
       PARTITION BY
         ty.actor_id
     ) <= 7 THEN 'average'
      WHEN AVG(ty.rating) OVER (
       PARTITION BY
         ty.actor_id
     ) <= 8 THEN 'good'
     WHEN AVG(ty.rating) OVER (
      PARTITION BY
         ty.actor_id
     ) > 8 THEN 'star'
   END AS quality_class
  , ty.year IS NOT NULL AS is_active
  , coalesce(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
-- Using full outer join to get all the rows from last_year and this year
