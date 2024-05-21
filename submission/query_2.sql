INSERT INTO
  actors WITH last_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1999
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      AVG(rating) AS avg_rating, -- Create an aggregation of film ratings for a given year.
      ARRAY_AGG(
        ROW(year, film, votes, rating, film_id)
      ) as films, -- Aggregate all of an actor's films for the given year.
      year
    FROM
      bootcamp.actor_films
    WHERE
      year = 2000
    GROUP BY
      actor,
      actor_id,
      year
  )
SELECT
  COALESCE(ty.actor, ly.actor) as actor,
  COALESCE(ty.actor_id, ly.actor_id) as actor_id,
  CASE
    -- Case 1: When the actor is not active this year, pull the films list from the cumulated history. 
    WHEN ty.year IS NULL THEN ly.films
    -- Case 2: When the actor is active this year, and there are films from last year, 
    -- add the films from this year to the list from the cumulated history.
    WHEN ty.year IS NOT NULL
    AND ly.films IS NOT NULL THEN ty.films || ly.films
    -- Case 3: When the actor is active this year, and there are no films from last year,
    -- (we're seeing this actor for the first time) then use only the list of films from this year.
    WHEN ty.year IS NOT NULL
    AND ly.films IS NULL THEN ty.films
  END as films,
  CASE
    WHEN ty.avg_rating > 8 THEN 'star'
    WHEN ty.avg_rating > 7 THEN 'good'
    WHEN ty.avg_rating > 6 THEN 'average'
    ELSE 'bad'
  END as quality_class,
  ty.year IS NOT NULL AS is_active, -- If the 'year' dimension is not null, then an actor was active that year.
  COALESCE(ty.year, ly.current_year + 1) as current_year
FROM
  last_year ly FULL
  OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id