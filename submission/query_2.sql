INSERT INTO alissabdeltoro.actors
-- Common Table Expression (CTE) to fetch data for the last year
WITH last_year AS (
    SELECT * 
    FROM alissabdeltoro.actors
    WHERE current_year = 2017  
    AND actor IN ('Lance Henriksen', 'William Shatner')
),
-- Common Table Expression (CTE) to fetch data for the current year and calculate average ratings
this_year AS (
    SELECT 
        actor,
        actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        year AS current_year  
    FROM bootcamp.actor_films
    WHERE year = 2018  
    GROUP BY actor, actor_id, year
),
-- Common Table Expression (CTE) to calculate average ratings for actors in the current year
avg_ratings AS (
    SELECT 
        actor_id,
        AVG(rating) AS avg_rating
    FROM bootcamp.actor_films
    GROUP BY actor_id
)
-- Main query to select and manipulate the data
SELECT 
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ty.current_year IS NULL THEN ly.films  
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NULL) THEN ty.films
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NOT NULL) THEN ty.films || ly.films  
    END AS films,
    CASE
        WHEN ar.avg_rating > 8 THEN 'star'  
        WHEN ar.avg_rating > 7 THEN 'good'  
        WHEN ar.avg_rating > 6 THEN 'average'  
        ELSE 'bad'  
    END AS quality_class,
    ty.current_year IS NOT NULL AS is_active,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id  
LEFT JOIN avg_ratings AS ar ON COALESCE(ly.actor_id, ty.actor_id) = ar.actor_id
