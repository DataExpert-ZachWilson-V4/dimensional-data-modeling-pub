-- Write a query that populates the actors table one year at a time.
-- Here we will be loading the actors data for year 2019 , 2020 and 2021
INSERT INTO actors
WITH last_year AS 
(
   -- Select data for the previous year (2019) from the actors table

    SELECT *
    FROM actors
    WHERE current_year = 2019
),
this_year AS 
(
    -- Select data for the current year (2020) from the actor_films table

    SELECT *
    FROM bootcamp.actor_films
    WHERE year = 2020
),
avg_rating AS
(
    -- Calculate the average rating of films for the current year

    SELECT COALESCE(AVG(ts.rating), 0) AS avg_rating
    FROM this_year ts
)
SELECT 

  -- Combine data from the previous year and the current year.

  COALESCE(ls.actor, ts.actor) AS actor,
  COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
  CASE
    WHEN ts.film IS NULL THEN ls.films
    WHEN ts.film IS NOT NULL AND ls.films IS NULL THEN ARRAY[
      CAST(ROW(
        ts.film,
        ts.year,
        ts.votes,
        ts.rating,
        ts.film_id
      ) AS ROW(film VARCHAR, year INTEGER, votes INTEGER, rating DOUBLE, film_id VARCHAR))
    ]
    WHEN ts.film IS NOT NULL AND ls.films IS NOT NULL THEN ARRAY[
      CAST(ROW(
        ts.film,
        ts.year,
        ts.votes,
        ts.rating,
        ts.film_id
      ) AS ROW(film VARCHAR, year INTEGER, votes INTEGER, rating DOUBLE, film_id VARCHAR))
    ] || ls.films
  END AS films,
  CASE

  -- Determine the quality class based on average rating or use the previous year's quality class.

    WHEN avg_rating > 0 THEN
      CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 THEN 'good'
        WHEN avg_rating > 6 THEN 'average'
        ELSE 'bad'
      END
    ELSE ls.quality_class
  END AS quality_class,
  -- Determine if the actor is active based on the presence of films data for the current year.
  ts.film IS NOT NULL AS is_active,
  CASE WHEN ts.film IS NOT NULL THEN 0 ELSE ls.years_since_last_active + 1 END AS years_since_last_active,
  COALESCE(ts.year, ls.current_year + 1) AS current_year
FROM last_year ls 
FULL OUTER JOIN this_year ts ON ls.actor = ts.actor
CROSS JOIN avg_rating