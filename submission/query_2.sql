  INSERT INTO tejalscr.actors
  WITH
  last_year AS (
    SELECT
      *
    FROM
      tejalscr.actors
    WHERE
      current_year = 1913
  ),
  this_year AS ( -- get aggregated data for avg rating for each actor per year
     SELECT
      actor
     ,actor_id 
     ,ARRAY_AGG(DISTINCT ROW(film, votes, rating, film_id)) as films
     ,AVG(rating) as avg_rating
     ,year as current_year
    FROM 
      bootcamp.actor_films
    WHERE
      year = 1914
    group by actor,actor_id,year
  )
SELECT 
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE WHEN ty.films IS NOT NULL THEN ty.films
       WHEN ly.films IS NOT NULL THEN ly.films
       WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films END AS films,
  CASE WHEN ty.avg_rating IS NOT NULL THEN
      CASE WHEN ty.avg_rating > 8 THEN 'star'
           WHEN ty.avg_rating > 7 THEN 'good'
           WHEN ty.avg_rating > 6 THEN 'average'
           WHEN ty.avg_rating <= 6 THEN 'bad' END
      ELSE ly.quality_class END as quality_class,
  ty.current_year IS NOT NULL AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
