INSERT INTO tharwaninitin.actors
WITH
  last_year AS (
    SELECT *
    FROM tharwaninitin.actors
    WHERE current_year = 1913
  ),
  this_year AS (
    SELECT actor, actor_id, year, ARRAY_AGG( ROW(film, votes, rating, film_id, year) ) films
    FROM bootcamp.actor_films
    WHERE year = 1914
    GROUP BY actor, actor_id, year
  ),
  joined_dataset AS (
    SELECT
      COALESCE(ty.actor, ly.actor) AS actor,
      COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
      COALESCE(ty.year, ly.current_year + 1) AS current_year,
      ty.year IS NOT NULL AS is_active,
      CASE
        WHEN ty.year IS NULL THEN ly.films
        WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
        WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
      END AS films
    FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
  ),
  quality_class_dataset as (
    SELECT *,
      REDUCE(
       FILTER(films, x -> x[5] = current_year),
       CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
       (s, x) -> CAST(ROW(x[3] + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
       s -> IF(s.count = 0, NULL, s.sum / s.count)
      ) as quality_class_avg
    FROM joined_dataset
  )
  SELECT actor, actor_id, films,
    CASE
      WHEN quality_class_avg > 8 THEN 'star'
      WHEN quality_class_avg > 7 AND quality_class_avg <= 8 THEN 'good'
      WHEN quality_class_avg > 6 AND quality_class_avg <= 7 THEN 'average'
      WHEN quality_class_avg <= 6 THEN 'bad'
    END AS quality_class,
    is_active,
    current_year
  FROM quality_class_dataset