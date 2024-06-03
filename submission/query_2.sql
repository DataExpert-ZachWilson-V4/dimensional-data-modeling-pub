INSERT INTO nattyd.actors
WITH last_year AS (
    SELECT * FROM nattyd.actors
    WHERE current_year = 1913 --1 of 2: Modify this to backfill.
),
this_year AS (
    SELECT DISTINCT
      actor,
      actor_id AS actorid,
      ARRAY_AGG(
        DISTINCT ROW(
          film,
          votes,
          rating,
          film_id
        )
      ) AS films,
      ARRAY_AGG(rating) AS rating,
      year AS current_year
    FROM bootcamp.actor_films
    WHERE year = 1914 --2 of 2: And modify this to backfill.
    GROUP BY actor_id, actor, year
)

SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actorid, ty.actorid) AS actorid,
  CASE 
    WHEN ty.films IS NULL
      THEN ly.films
    WHEN ty.films IS NOT NULL
      AND ly.films IS NULL 
      THEN ty.films
    WHEN ty.films IS NOT NULL
      AND ly.films IS NOT NULL
      THEN array_distinct(ty.films || ly.films)
  END AS films,
  COALESCE( 
    CASE
      WHEN 
        REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
          > 8
        THEN 'star'
      WHEN
        REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
          > 7
        AND 
          REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
            <= 8
        THEN 'good'
      WHEN
        REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
          > 6
        AND 
          REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
            <= 7
        THEN 'average'
      WHEN
        REDUCE(ty.rating, 0, (s, x) -> s + x, s -> s) / cardinality(ty.rating) 
          <= 6
        THEN 'bad'
    END,
    ly.quality_class
  ) AS quality_class,
  ty.films IS NOT NULL AS is_active,
  COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty 
  ON TRIM(ly.actorid) = TRIM(ty.actorid)
