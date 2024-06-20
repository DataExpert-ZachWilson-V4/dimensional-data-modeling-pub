-- 1914 - 2021
INSERT INTO changtiange199881320.actors

WITH last_year AS (
    SELECT *
    FROM changtiange199881320.actors
    WHERE current_year = 1913 -- backfill from here to 2020
), 
this_year AS (
    SELECT *
    FROM bootcamp.actor_films
    WHERE year = 1914 -- backfill from here to 2021
), 
film_data AS (
    SELECT
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        COALESCE(ly.actor, ty.actor) AS actor, 
        ty.rating,
        CASE
            WHEN ty.film IS NULL THEN ly.films
            WHEN ty.film IS NOT NULL AND ly.films IS NULL THEN ARRAY[ 
                ROW(ty.film_id, ty.film, ty.votes, ty.rating, ty.year)
            ]
            WHEN ty.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[
                ROW(ty.film_id, ty.film, ty.votes, ty.rating, ty.year)
            ] || ly.films
        END AS film_data,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM
        last_year ly 
    FULL OUTER JOIN 
        this_year ty 
    ON
        ly.actor_id = ty.actor_id
), 
-- If not unnest, the reuslt will be [array of struct], and I cannot create 
-- array of struct in my DDL using films array<struct<xx:string, xx:double>>
unnested_film_data AS (
    SELECT
        actor_id, actor, film.film_id,
        film.film, film.votes,
        film.rating, film.year, 
        is_active, current_year
    FROM 
        film_data,
        UNNEST(film_data) AS film(film_id, film, votes, rating, year)
), 
-- If not select distinct, the result after backfill will be duplicated. 
distinct_films AS(
    SELECT DISTINCT 
        actor_id, actor, film_id,
        film, votes, rating, year, 
        is_active, current_year
    FROM
        unnested_film_data
)
SELECT
    actor_id, actor,
    ARRAY_AGG(ROW(film_id, film, votes, rating, year)) AS films,
    CASE
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
        WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(is_active) AS is_active,
    current_year
FROM 
    distinct_films
GROUP BY 
    actor_id, 
    actor, 
    current_year

-- SHOW STATS FOR (
--     SELECT
--          * 
--      FROM 
--          changtiange199881320.actors
--      WHERE 
--          current_year = 2021
--   )