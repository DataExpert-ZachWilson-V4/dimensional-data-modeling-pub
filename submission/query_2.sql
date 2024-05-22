INSERT INTO purvabadve55523.actors
 -- last_year CTE captures all rows from the actors table where current_year is 1914
WITH
  last_year AS (
    SELECT
      *
    FROM
      purvabadve55523.actors
    WHERE
      current_year = 1914
  ),
  this_year AS (
    SELECT
        actor,
        actor_id,
        year,
-- Aggregate films information into an array of rows for each actor
        ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                film_id,
                year
            )
        ) AS films,
-- Calculate the weighted average rating for each actor's films in 1915
        SUM(votes * rating) / SUM(votes) as avg_rating
    FROM bootcamp.actor_films
    WHERE year = 1915
    GROUP BY actor, actor_id, year
)

SELECT
    COALESCE(l.actor, t.actor) AS actor,
    COALESCE(l.actor_id, t.actor_id) AS actor_id,
    CASE
        WHEN t.films IS NULL THEN l.films
        WHEN l.films IS NULL THEN t.films
        WHEN t.films IS NOT NULL AND l.films IS NOT NULL 
            THEN (t.films || l.films)
    END AS films,
    CASE
        WHEN t.avg_rating > 8 THEN 'star'
        WHEN t.avg_rating > 7 THEN 'good'
        WHEN t.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    CASE
        WHEN t.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
 -- Set current_year to the year from this_year if available, otherwise increment the year from last_year by 1
    COALESCE(t.year, l.current_year + 1) AS current_year 
  FROM last_year l
  FULL OUTER JOIN
    this_year t
    ON l.actor_id = t.actor_id
