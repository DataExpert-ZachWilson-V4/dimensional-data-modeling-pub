INSERT INTO alissabdeltoro.actors (actor, actor_id, films, quality_class, is_active, current_year)


-- Common Table Expression (CTE) to fetch data for the last year
WITH last_year AS (
    SELECT * 
    FROM alissabdeltoro.actors
    WHERE current_year = 2019
),
-- Common Table Expression (CTE) to fetch data for the current year and calculate average ratings
this_year AS (
    SELECT 
        actor,
        actor_id,
        AVG(rating) AS avg_rating,
        ARRAY_AGG(ROW(year, film, votes, rating, film_id)) AS films,
        year AS current_year  
    FROM bootcamp.actor_films
    WHERE year = 2020
    GROUP BY actor, actor_id, year
)

-- Inserting the results into the alissabdeltoro.actors table
SELECT 
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ty.current_year IS NULL THEN ly.films  
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NULL) THEN ty.films
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NOT NULL) THEN ty.films || ly.films  
    END AS films,
    CASE
        WHEN ty.current_year IS NULL THEN ly.quality_class
        WHEN ty.avg_rating > 8 THEN 'star'  
        WHEN ty.avg_rating > 7 THEN 'good'  
        WHEN ty.avg_rating > 6 THEN 'average' 
        WHEN ty.avg_rating <= 6 THEN 'bad' 
        ELSE NULL
    END AS quality_class,
    ty.current_year IS NOT NULL AS is_active,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
