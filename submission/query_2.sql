-- 2021
-- 1936
-- 1937
INSERT INTO changtiange199881320.actors

WITH last_year AS (
    SELECT *
    FROM changtiange199881320.actors
    WHERE current_year = 1914
), 
this_year AS(
    SELECT *
    FROM bootcamp.actor_films
    WHERE year = 1915
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor, 

    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,

    CASE 
        WHEN ty.film IS NULL THEN ly.films
        WHEN ty.film IS NOT NULL AND ly.films IS NULL 
            THEN ARRAY[
                ROW(
                    ty.film, ty.votes, 
                    ty.rating, ty.film_id 
                )
            ]
        WHEN ty.film IS NOT NULL AND ly.films IS NOT NULL 
            THEN ARRAY[
                ROW(
                    ty.film, ty.votes, 
                    ty.rating, ty.film_id 
                )
            ] || ly.films
    END AS films, 

    CASE
        WHEN ty.rating > 8 THEN 'star'
        WHEN ty.rating > 7 AND ty.rating <= 8 THEN 'good'
        WHEN ty.rating > 6 AND ty.rating <= 7 THEN 'good'
        WHEN ty.rating <= 6 THEN 'bad'
    END AS quality_class, 


    ty.actor IS NOT NULL AS is_active, 

    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
    last_year ly 
FULL OUTER JOIN 
    this_year ty 
ON
    ly.actor_id = ty.actor_id