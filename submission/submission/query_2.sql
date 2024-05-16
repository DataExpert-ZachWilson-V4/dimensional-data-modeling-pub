-- Check what min and max years are
-- min_year = 1914 and max_year = 2021
SELECT MIN(year) AS min_year, MAX(year) AS max_year
FROM bootcamp.actor_films;

-- Repeat for all pairs between 1913 and 2021
INSERT INTO actors
WITH last_year AS (SELECT *
                   FROM actors
                   WHERE current_year = 1914),
     this_year AS (SELECT actor,
                          actor_id,
                          flatten(array_agg(ARRAY[ROW(film, year, votes, rating, film_id)])) AS films,
                          year,
                          AVG (rating) AS avg_rating
                    FROM bootcamp.actor_films
                    WHERE year = 1915
                    GROUP BY actor, actor_id, year)

SELECT COALESCE(ly.actor, ty.actor)           AS actor,
       COALESCE(ly.actor_id, ty.actor_id)     AS actor_id,
       CASE
           WHEN ty.year IS NULL THEN ly.films
           WHEN ty.year IS NOT NULL
               AND ly.films IS NULL THEN ty.films
           WHEN ty.year IS NOT NULL
               AND ly.films IS NOT NULL THEN ty.films || ly.films
           END                                AS films,
       CASE
           WHEN ty.year IS NULL THEN ly.quality_class
           ELSE
               CASE
                   WHEN ty.avg_rating > 8 THEN 'star'
                   WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
                   WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
                   WHEN ty.avg_rating <= 6 THEN 'bad'
                   END
           END                                AS quality_class,
       ty.year IS NOT NULL                    AS is_active,
       COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
         FULL OUTER JOIN this_year ty
                         ON ly.actor = ty.actor