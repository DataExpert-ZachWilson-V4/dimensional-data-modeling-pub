--     Subqueries:
--         last_year: Selects actor data from alia.actors where current_year is 2000.
--         this_year: Aggregates film data for actors from bootcamp.actor_films where the year is 2001. Calculates the quality_class based on average film ratings.

--     Main logic:
--         Combines results from last_year (ly) and this_year (ty) using a full outer join on actor_id.
--         Selects the actor and actor_id from either the previous or current year.
--         Merges film arrays from both years.
--         Determines quality_class from the current year if available; otherwise, uses the previous year's class.
--         Sets is_active based on the availability of data for the current year.
--         Updates current_year to the current year or increments it from the previous year.


INSERT INTO
  alia.actors
WITH
  last_year AS (
    SELECT
      actor,
      actor_id,
      films,
      quality_class,
      is_active,
      current_year
    FROM
      alia.actors
    WHERE
      current_year = 1999
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      year,
      array_agg (ROW (film, votes, rating, film_id,year)) films,
      CASE
        WHEN AVG(rating) > 8.0 THEN 'star'
        WHEN AVG(rating) > 7.0 AND AVG(rating) <= 8.0 THEN 'good'
        WHEN AVG(rating) > 6.0 AND AVG(rating) <= 7.0 THEN 'average'
        WHEN AVG(rating) <= 6.0 THEN 'bad'
      END as quality_class
    FROM
      bootcamp.actor_films
    WHERE
      year = 2000
    GROUP BY
      1,
      2,
      3
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL AND ly.current_year IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL AND ly.current_year IS NOT NULL THEN ty.films || ly.films
  END AS films,
  COALESCE(ty.quality_class, ly.quality_class) quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly -- ly = last year
  FULL OUTER JOIN this_year ty --  ty = this year
  ON ly.actor_id = ty.actor_id