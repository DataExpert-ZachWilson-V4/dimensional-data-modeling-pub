
INSERT INTO derekleung.actors
--Logic: 
--Here we are assuming 2021 snapshots has come in and we are adding it into previous cumulative data
--To change the year, we can declare a integer variable called update_year, and change the where filters in the definition of last_years and this_year
--last_years is cumulative data until 2nd most recent year
--this_year is new snapshots to be cumulated, already morphed into structure alike last_years for convenience (note window functions are delibrately used over group by's, if any grader see this please give feedback on how to handle non-aggregating columns like actor)
--SELECT query: use concatenate to update columns requiring cumulated data (in this case, only films), use COALESCE to update everything else
WITH
  last_years AS (
    SELECT
      *
    FROM
      derekleung.actors
    WHERE
      current_year = 2020
  ),
  --Note: The point of row_number() here is to dedup, so no particular order is really required
  this_year_unfiltered AS (
    SELECT
      first_value(actor) over w AS actor,
      actor_id,
      ARRAY_AGG(
        ROW(
          film, 
          film_id, 
          votes, 
          rating
        )
      ) over w AS film_new_update,
      CASE
        WHEN SUM(votes * rating) over w / SUM(votes) over w > 8 THEN 'star'
        WHEN SUM(votes * rating) over w / SUM(votes) over w > 7 THEN 'good'
        WHEN SUM(votes * rating) over w / SUM(votes) over w > 6 THEN 'average'
        ELSE 'bad'
      END AS quality_class,
      TRUE AS is_active,
      first_value(YEAR) over w AS year,
      row_number() over w AS row_count
    FROM
      derekleung.actor_films
    WHERE
      YEAR = 2021 and row_count = 1
    WINDOW w as (partition by actor_id)
    ORDER BY actor_id
  )
  --Join last year's cumulative and today's snapshot
SELECT
  COALESCE(lys.actor, ty.actor) AS actor,
  COALESCE(lys.actor_id, ty.actor_id) AS actor_id,
--accummulate film history for every actor: new actors or old actors inactive this year don't need any concatenation, while old actors active this year needs to concatenate film history of this year on top
  CASE
    WHEN ty.film_new_update IS NULL THEN lys.films
    WHEN ty.film_new_update IS NOT NULL
    AND lys.films IS NULL THEN ty.film_new_update
    WHEN ty.film_new_update IS NOT NULL
    AND lys.films IS NOT NULL THEN ty.film_new_update || lys.films
  END AS films,
  COALESCE(ty.quality_class,lys.quality_class) AS quality_class,
  ty.film_new_update IS NOT NULL AS is_active,
  COALESCE(ty.year, lys.current_year + 1) AS current_year
FROM
  last_years lys
  FULL OUTER JOIN this_year ty ON lys.actor_id = ty.actor_id
--To introduce some kind of order while viewing the table, not mandatory
ORDER BY
  actor
