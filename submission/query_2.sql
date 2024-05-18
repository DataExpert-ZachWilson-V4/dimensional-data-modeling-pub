INSERT INTO actors
WITH last_year_films AS (
  SELECT
    *
  FROM
    actors
  WHERE
    current_year = 2011
),
this_year_films AS (
  SELECT
    actor,
    actor_id,
    YEAR,
    AVG(rating) AS average_rating,
    ARRAY_AGG(
      ROW(
        ts.film,
        ts.year,
        ts.votes,
        ts.rating,
        ts.film_id
      )
    ) AS films
  FROM
    bootcamp.actor_films ts
  WHERE
    YEAR = 2012
  GROUP BY
    actor,
    actor_id,
    YEAR
)
SELECT
  COALESCE(ls.actor, ts.actor) AS actor,
  COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
  -- Merging film data from both years
  CASE
      WHEN ts.year IS NULL THEN ls.films  -- Use last year's films if no films for the current year
      WHEN ts.year IS NOT NULL AND ls.films IS NULL THEN ts.films  -- Use current year's films if no last year's films
      WHEN ts.year IS NOT NULL AND ls.films IS NOT NULL THEN ts.films || ls.films  -- Concatenate films if both years have films
  END AS films,
  CASE
    WHEN ts.average_rating > 8 THEN 'star'
    WHEN ts.average_rating > 7 AND ts.average_rating <= 8 THEN 'good'
    WHEN ts.average_rating > 6 AND ts.average_rating <= 7 THEN 'average'
    WHEN ts.average_rating <= 6 THEN 'bad'
  END AS quality_class,
  ts.year IS NOT NULL AS is_active,
  COALESCE(ts.year, ls.current_year + 1) AS current_year
FROM
  last_year_films ls
FULL OUTER JOIN this_year_films ts
ON ls.actor_id = ts.actor_id
