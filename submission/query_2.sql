--Cumulative Table Computation Query (query_2)
--I am cumulating the data from year 1914 to 1921
insert into hariomnayani88482.actors
WITH
  last_year AS (
    SELECT
      *
    FROM
      hariomnayani88482.actors
    WHERE
      current_year = 1913
  ),
  this_year AS (
    SELECT
      array_agg(row(film, votes, rating, film_id, YEAR)) AS films,
      actor,
      actor_id,
      YEAR,
      CASE
    WHEN AVG(rating) > 8 THEN 'star'
    WHEN AVG(rating) > 7 THEN 'good'
    WHEN AVG(rating) > 6 THEN 'average'
    ELSE 'bad'
  END AS quality_class
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 1914
    GROUP BY
      actor,
      actor_id,
      YEAR
  )
SELECT
  coalesce(ly.actor, ty.actor) AS actor,
  coalesce(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
  --when there is no new film for an actor this year
    WHEN ly.films IS NULL THEN ty.films
    --when this is a new actor
    WHEN ty.films IS NULL then ly.films
    --when this is an new film for existing actor
    WHEN ty.films IS NOT NULL 
    AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  coalesce(ty.quality_class,ly.quality_class) as quality_class,
  ty.actor IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS currrent_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
